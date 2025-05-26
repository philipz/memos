package v1

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/lithammer/shortuuid/v4"
	"github.com/pkg/errors"
	"github.com/usememos/gomark/ast"
	"github.com/usememos/gomark/parser"
	"github.com/usememos/gomark/parser/tokenizer"
	"github.com/usememos/gomark/renderer"
	"github.com/usememos/gomark/restore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"bufio"
	"bytes"
	"encoding/json"
	"strings"

	"github.com/usememos/memos/plugin/webhook"
	apiv1 "github.com/usememos/memos/proto/gen/api/v1"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/server/runner/memopayload"
	"github.com/usememos/memos/store"
)

// AgentEventChannels manages channels for agent task events.
// It allows multiple clients to subscribe to events for the same agent task.
// TODO: Consider moving this to a more central place if other services need similar fan-out eventing.
type AgentEventChannels struct {
	mu       sync.RWMutex
	channels map[string]map[chan *apiv1.AgentTaskEvent]bool // agentTaskID -> set of channels
}

// NewAgentEventChannels creates a new AgentEventChannels manager.
func NewAgentEventChannels() *AgentEventChannels {
	return &AgentEventChannels{
		channels: make(map[string]map[chan *apiv1.AgentTaskEvent]bool),
	}
}

// Register adds a channel to the list of subscribers for an agent task.
func (aec *AgentEventChannels) Register(agentTaskID string, ch chan *apiv1.AgentTaskEvent) {
	aec.mu.Lock()
	defer aec.mu.Unlock()
	if aec.channels[agentTaskID] == nil {
		aec.channels[agentTaskID] = make(map[chan *apiv1.AgentTaskEvent]bool)
	}
	aec.channels[agentTaskID][ch] = true
	slog.Debug("Registered channel for agent task events", "agent_task_id", agentTaskID)
}

// Unregister removes a channel from the list of subscribers.
func (aec *AgentEventChannels) Unregister(agentTaskID string, ch chan *apiv1.AgentTaskEvent) {
	aec.mu.Lock()
	defer aec.mu.Unlock()
	if subscribers, ok := aec.channels[agentTaskID]; ok {
		delete(subscribers, ch)
		if len(subscribers) == 0 {
			delete(aec.channels, agentTaskID)
		}
	}
	// Note: The channel `ch` itself should be closed by the goroutine that created it (e.g., StreamAgentTaskEvents defer func).
	slog.Debug("Unregistered channel for agent task events", "agent_task_id", agentTaskID)
}

// Send broadcasts an event to all registered channels for an agent task.
func (aec *AgentEventChannels) Send(agentTaskID string, event *apiv1.AgentTaskEvent) {
	aec.mu.RLock()
	subscribers, ok := aec.channels[agentTaskID]
	if !ok {
		aec.mu.RUnlock()
		slog.Debug("No subscribers for agent task event", "agent_task_id", agentTaskID, "event_type", event.EventType)
		return
	}

	// Create a snapshot of channels to send to, to avoid holding lock during send
	chansToSend := make([]chan *apiv1.AgentTaskEvent, 0, len(subscribers))
	for ch := range subscribers {
		chansToSend = append(chansToSend, ch)
	}
	aec.mu.RUnlock()

	for _, ch := range chansToSend {
		select {
		case ch <- event:
		default:
			slog.Warn("Agent event channel full, unable to send event", "agent_task_id", agentTaskID, "event_type", event.EventType)
		}
	}
	slog.Debug("Sent agent task event to subscribers", "agent_task_id", agentTaskID, "event_type", event.EventType, "subscriber_count", len(chansToSend))
}

// CloseAndRemove closes all channels for a specific agent task and removes the task entry.
// This should be called when a task is definitively finished (completed, failed, canceled).
func (aec *AgentEventChannels) CloseAndRemove(agentTaskID string) {
	aec.mu.Lock()
	defer aec.mu.Unlock()

	if subscribers, ok := aec.channels[agentTaskID]; ok {
		for ch := range subscribers {
			close(ch) // Close each channel to signal consumers
		}
		delete(aec.channels, agentTaskID)
		slog.Info("Closed and removed all channels for agent task", "agent_task_id", agentTaskID)
	}
}

func (s *APIV1Service) CreateMemo(ctx context.Context, request *apiv1.CreateMemoRequest) (*apiv1.Memo, error) {
	user, err := s.GetCurrentUser(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get user")
	}

	create := &store.Memo{
		UID:        shortuuid.New(),
		CreatorID:  user.ID,
		Content:    request.Memo.Content,
		Visibility: convertVisibilityToStore(request.Memo.Visibility),
	}
	workspaceMemoRelatedSetting, err := s.Store.GetWorkspaceMemoRelatedSetting(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get workspace memo related setting")
	}
	if workspaceMemoRelatedSetting.DisallowPublicVisibility && create.Visibility == store.Public {
		return nil, status.Errorf(codes.PermissionDenied, "disable public memos system setting is enabled")
	}
	contentLengthLimit, err := s.getContentLengthLimit(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get content length limit")
	}
	if len(create.Content) > contentLengthLimit {
		return nil, status.Errorf(codes.InvalidArgument, "content too long (max %d characters)", contentLengthLimit)
	}
	if err := memopayload.RebuildMemoPayload(create); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to rebuild memo payload: %v", err)
	}
	if request.Memo.Location != nil {
		create.Payload.Location = convertLocationToStore(request.Memo.Location)
	}

	memo, err := s.Store.CreateMemo(ctx, create)
	if err != nil {
		return nil, err
	}
	if len(request.Memo.Resources) > 0 {
		_, err := s.SetMemoResources(ctx, &apiv1.SetMemoResourcesRequest{
			Name:      fmt.Sprintf("%s%s", MemoNamePrefix, memo.UID),
			Resources: request.Memo.Resources,
		})
		if err != nil {
			return nil, errors.Wrap(err, "failed to set memo resources")
		}
	}
	if len(request.Memo.Relations) > 0 {
		_, err := s.SetMemoRelations(ctx, &apiv1.SetMemoRelationsRequest{
			Name:      fmt.Sprintf("%s%s", MemoNamePrefix, memo.UID),
			Relations: request.Memo.Relations,
		})
		if err != nil {
			return nil, errors.Wrap(err, "failed to set memo relations")
		}
	}

	memoMessage, err := s.convertMemoFromStore(ctx, memo)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert memo")
	}
	// Try to dispatch webhook when memo is created.
	if err := s.DispatchMemoCreatedWebhook(ctx, memoMessage); err != nil {
		slog.Warn("Failed to dispatch memo created webhook", slog.Any("err", err))
	}

	return memoMessage, nil
}

func (s *APIV1Service) ListMemos(ctx context.Context, request *apiv1.ListMemosRequest) (*apiv1.ListMemosResponse, error) {
	memoFind := &store.FindMemo{
		// Exclude comments by default.
		ExcludeComments: true,
	}
	if err := s.buildMemoFindWithFilter(ctx, memoFind, request.OldFilter); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to build find memos with filter: %v", err)
	}
	if request.Parent != "" && request.Parent != "users/-" {
		userID, err := ExtractUserIDFromName(request.Parent)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid parent: %v", err)
		}
		memoFind.CreatorID = &userID
		memoFind.OrderByPinned = true
	}
	if request.State == apiv1.State_ARCHIVED {
		state := store.Archived
		memoFind.RowStatus = &state
	} else {
		state := store.Normal
		memoFind.RowStatus = &state
	}
	if request.Direction == apiv1.Direction_ASC {
		memoFind.OrderByTimeAsc = true
	}
	if request.Filter != "" {
		if err := s.validateFilter(ctx, request.Filter); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid filter: %v", err)
		}
		memoFind.Filter = &request.Filter
	}

	currentUser, err := s.GetCurrentUser(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get user")
	}
	if currentUser == nil {
		memoFind.VisibilityList = []store.Visibility{store.Public}
	} else {
		if memoFind.CreatorID == nil {
			internalFilter := fmt.Sprintf(`creator_id == %d || visibility in ["PUBLIC", "PROTECTED"]`, currentUser.ID)
			if memoFind.Filter != nil {
				filter := fmt.Sprintf("(%s) && (%s)", *memoFind.Filter, internalFilter)
				memoFind.Filter = &filter
			} else {
				memoFind.Filter = &internalFilter
			}
		} else if *memoFind.CreatorID != currentUser.ID {
			memoFind.VisibilityList = []store.Visibility{store.Public, store.Protected}
		}
	}

	workspaceMemoRelatedSetting, err := s.Store.GetWorkspaceMemoRelatedSetting(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get workspace memo related setting")
	}
	if workspaceMemoRelatedSetting.DisplayWithUpdateTime {
		memoFind.OrderByUpdatedTs = true
	}

	var limit, offset int
	if request.PageToken != "" {
		var pageToken apiv1.PageToken
		if err := unmarshalPageToken(request.PageToken, &pageToken); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page token: %v", err)
		}
		limit = int(pageToken.Limit)
		offset = int(pageToken.Offset)
	} else {
		limit = int(request.PageSize)
	}
	if limit <= 0 {
		limit = DefaultPageSize
	}
	limitPlusOne := limit + 1
	memoFind.Limit = &limitPlusOne
	memoFind.Offset = &offset
	memos, err := s.Store.ListMemos(ctx, memoFind)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list memos: %v", err)
	}

	memoMessages := []*apiv1.Memo{}
	nextPageToken := ""
	if len(memos) == limitPlusOne {
		memos = memos[:limit]
		nextPageToken, err = getPageToken(limit, offset+limit)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get next page token, error: %v", err)
		}
	}
	for _, memo := range memos {
		memoMessage, err := s.convertMemoFromStore(ctx, memo)
		if err != nil {
			return nil, errors.Wrap(err, "failed to convert memo")
		}
		memoMessages = append(memoMessages, memoMessage)
	}

	response := &apiv1.ListMemosResponse{
		Memos:         memoMessages,
		NextPageToken: nextPageToken,
	}
	return response, nil
}

func (s *APIV1Service) GetMemo(ctx context.Context, request *apiv1.GetMemoRequest) (*apiv1.Memo, error) {
	memoUID, err := ExtractMemoUIDFromName(request.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid memo name: %v", err)
	}
	memo, err := s.Store.GetMemo(ctx, &store.FindMemo{
		UID: &memoUID,
	})
	if err != nil {
		return nil, err
	}
	if memo == nil {
		return nil, status.Errorf(codes.NotFound, "memo not found")
	}
	if memo.Visibility != store.Public {
		user, err := s.GetCurrentUser(ctx)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get user")
		}
		if user == nil {
			return nil, status.Errorf(codes.PermissionDenied, "permission denied")
		}
		if memo.Visibility == store.Private && memo.CreatorID != user.ID {
			return nil, status.Errorf(codes.PermissionDenied, "permission denied")
		}
	}

	memoMessage, err := s.convertMemoFromStore(ctx, memo)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert memo")
	}
	return memoMessage, nil
}

func (s *APIV1Service) UpdateMemo(ctx context.Context, request *apiv1.UpdateMemoRequest) (*apiv1.Memo, error) {
	user, err := s.GetCurrentUser(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get current user for update: %v", err)
	}
	if user == nil {
		return nil, status.Errorf(codes.Unauthenticated, "user not authenticated for update")
	}

	if len(request.UpdateMask.Paths) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "update mask is empty")
	}

	memoUID, err := ExtractMemoUIDFromName(request.Memo.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid memo name: %v", err)
	}
	memo, err := s.Store.GetMemo(ctx, &store.FindMemo{
		UID: &memoUID,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get memo for update")
	}
	if memo == nil {
		return nil, status.Errorf(codes.NotFound, "memo not found for update")
	}

	if memo.CreatorID != user.ID && !isSuperUser(user) {
		return nil, status.Errorf(codes.PermissionDenied, "permission denied to update memo")
	}

	update := &store.UpdateMemo{
		ID: memo.ID,
	}

	for _, path := range request.UpdateMask.Paths {
		if path == "content" {
			update.Content = &request.Memo.Content
		} else if path == "visibility" {
			visibility := convertVisibilityToStore(request.Memo.Visibility)
			update.Visibility = &visibility
		} else if path == "pinned" {
			update.Pinned = &request.Memo.Pinned
		} else if path == "resources" {
			_, err := s.SetMemoResources(ctx, &apiv1.SetMemoResourcesRequest{
				Name:      request.Memo.Name,
				Resources: request.Memo.Resources,
			})
			if err != nil {
				return nil, errors.Wrap(err, "failed to set memo resources during update")
			}
		} else if path == "relations" {
			_, err := s.SetMemoRelations(ctx, &apiv1.SetMemoRelationsRequest{
				Name:      request.Memo.Name,
				Relations: request.Memo.Relations,
			})
			if err != nil {
				return nil, errors.Wrap(err, "failed to set memo relations during update")
			}
		} else if path == "agent_task_id" {
			update.AgentTaskID = request.Memo.AgentTaskId
		} else if path == "agent_status" {
			if request.Memo.AgentStatus != nil {
				storeStatus := convertAgentStatusToStore(*request.Memo.AgentStatus)
				update.AgentStatus = &storeStatus
			}
		} else if path == "agent_query_text" {
			update.AgentQueryText = request.Memo.AgentQueryText
		} else if path == "agent_plan_json" {
			update.AgentPlanJson = request.Memo.AgentPlanJson
		} else if path == "agent_steps_json" {
			update.AgentStepsJson = request.Memo.AgentStepsJson
		} else if path == "agent_result_json" {
			update.AgentResultJson = request.Memo.AgentResultJson
		} else if path == "agent_error_message" {
			update.AgentErrorMessage = request.Memo.AgentErrorMessage
		}
	}

	if err = s.Store.UpdateMemo(ctx, update); err != nil {
		// Attempt to fetch the memo even if update failed to return its current state
		currentMemo, getErr := s.Store.GetMemo(ctx, &store.FindMemo{ID: &memo.ID})
		if getErr != nil {
			slog.Error("Failed to get memo after update attempt failed", "update_error", err, "get_error", getErr)
			return nil, status.Errorf(codes.Internal, "failed to update memo and then failed to retrieve its current state: update_err=%v, get_err=%v", err, getErr)
		}
		memoMessage, convertErr := s.convertMemoFromStore(ctx, currentMemo)
		if convertErr != nil {
			slog.Error("Failed to convert memo after update attempt failed", "update_error", err, "convert_error", convertErr)
			return nil, status.Errorf(codes.Internal, "failed to update memo and then failed to convert its current state: update_err=%v, convert_err=%v", err, convertErr)
		}
		// It's debatable whether to dispatch a webhook if the primary update failed.
		// For now, we'll return the current state and the original error.
		return memoMessage, status.Errorf(codes.Internal, "failed to update memo: %v. Current memo state returned.", err)
	}

	updatedMemoStore, err := s.Store.GetMemo(ctx, &store.FindMemo{ID: &memo.ID})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get memo after update")
	}
	memoMessage, err := s.convertMemoFromStore(ctx, updatedMemoStore)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert memo after update")
	}
	if err := s.DispatchMemoUpdatedWebhook(ctx, memoMessage); err != nil {
		slog.Warn("Failed to dispatch memo updated webhook after update", slog.Any("err", err))
	}
	return memoMessage, nil
}

func (s *APIV1Service) DeleteMemo(ctx context.Context, request *apiv1.DeleteMemoRequest) (*emptypb.Empty, error) {
	memoUID, err := ExtractMemoUIDFromName(request.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid memo name: %v", err)
	}
	memo, err := s.Store.GetMemo(ctx, &store.FindMemo{
		UID: &memoUID,
	})
	if err != nil {
		return nil, err
	}
	if memo == nil {
		return nil, status.Errorf(codes.NotFound, "memo not found")
	}

	user, err := s.GetCurrentUser(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get current user")
	}
	// Only the creator or admin can update the memo.
	if memo.CreatorID != user.ID && !isSuperUser(user) {
		return nil, status.Errorf(codes.PermissionDenied, "permission denied")
	}

	if memoMessage, err := s.convertMemoFromStore(ctx, memo); err == nil {
		// Try to dispatch webhook when memo is deleted.
		if err := s.DispatchMemoDeletedWebhook(ctx, memoMessage); err != nil {
			slog.Warn("Failed to dispatch memo deleted webhook", slog.Any("err", err))
		}
	}

	if err = s.Store.DeleteMemo(ctx, &store.DeleteMemo{ID: memo.ID}); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete memo")
	}

	// Delete memo relation
	if err := s.Store.DeleteMemoRelation(ctx, &store.DeleteMemoRelation{MemoID: &memo.ID}); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete memo relations")
	}

	// Delete related resources.
	resources, err := s.Store.ListResources(ctx, &store.FindResource{MemoID: &memo.ID})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list resources")
	}
	for _, resource := range resources {
		if err := s.Store.DeleteResource(ctx, &store.DeleteResource{ID: resource.ID}); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to delete resource")
		}
	}

	// Delete memo comments
	commentType := store.MemoRelationComment
	relations, err := s.Store.ListMemoRelations(ctx, &store.FindMemoRelation{RelatedMemoID: &memo.ID, Type: &commentType})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list memo comments")
	}
	for _, relation := range relations {
		if err := s.Store.DeleteMemo(ctx, &store.DeleteMemo{ID: relation.MemoID}); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to delete memo comment")
		}
	}

	// Delete memo references
	referenceType := store.MemoRelationReference
	if err := s.Store.DeleteMemoRelation(ctx, &store.DeleteMemoRelation{RelatedMemoID: &memo.ID, Type: &referenceType}); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete memo references")
	}

	return &emptypb.Empty{}, nil
}

func (s *APIV1Service) CreateMemoComment(ctx context.Context, request *apiv1.CreateMemoCommentRequest) (*apiv1.Memo, error) {
	memoUID, err := ExtractMemoUIDFromName(request.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid memo name: %v", err)
	}
	relatedMemo, err := s.Store.GetMemo(ctx, &store.FindMemo{UID: &memoUID})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get memo")
	}

	// Create the memo comment first.
	memoComment, err := s.CreateMemo(ctx, &apiv1.CreateMemoRequest{Memo: request.Comment})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create memo")
	}
	memoUID, err = ExtractMemoUIDFromName(memoComment.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid memo name: %v", err)
	}
	memo, err := s.Store.GetMemo(ctx, &store.FindMemo{UID: &memoUID})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get memo")
	}

	// Build the relation between the comment memo and the original memo.
	_, err = s.Store.UpsertMemoRelation(ctx, &store.MemoRelation{
		MemoID:        memo.ID,
		RelatedMemoID: relatedMemo.ID,
		Type:          store.MemoRelationComment,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create memo relation")
	}
	creatorID, err := ExtractUserIDFromName(memoComment.Creator)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid memo creator")
	}
	if memoComment.Visibility != apiv1.Visibility_PRIVATE && creatorID != relatedMemo.CreatorID {
		activity, err := s.Store.CreateActivity(ctx, &store.Activity{
			CreatorID: creatorID,
			Type:      store.ActivityTypeMemoComment,
			Level:     store.ActivityLevelInfo,
			Payload: &storepb.ActivityPayload{
				MemoComment: &storepb.ActivityMemoCommentPayload{
					MemoId:        memo.ID,
					RelatedMemoId: relatedMemo.ID,
				},
			},
		})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create activity")
		}
		if _, err := s.Store.CreateInbox(ctx, &store.Inbox{
			SenderID:   creatorID,
			ReceiverID: relatedMemo.CreatorID,
			Status:     store.UNREAD,
			Message: &storepb.InboxMessage{
				Type:       storepb.InboxMessage_MEMO_COMMENT,
				ActivityId: &activity.ID,
			},
		}); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create inbox")
		}
	}

	return memoComment, nil
}

func (s *APIV1Service) ListMemoComments(ctx context.Context, request *apiv1.ListMemoCommentsRequest) (*apiv1.ListMemoCommentsResponse, error) {
	memoUID, err := ExtractMemoUIDFromName(request.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid memo name: %v", err)
	}
	memo, err := s.Store.GetMemo(ctx, &store.FindMemo{UID: &memoUID})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get memo")
	}

	currentUser, err := s.GetCurrentUser(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get user")
	}
	var memoFilter string
	if currentUser == nil {
		memoFilter = `visibility == "PUBLIC"`
	} else {
		memoFilter = fmt.Sprintf(`creator_id == %d || visibility in ["PUBLIC", "PROTECTED"]`, currentUser.ID)
	}
	memoRelationComment := store.MemoRelationComment
	memoRelations, err := s.Store.ListMemoRelations(ctx, &store.FindMemoRelation{
		RelatedMemoID: &memo.ID,
		Type:          &memoRelationComment,
		MemoFilter:    &memoFilter,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list memo relations")
	}

	var memos []*apiv1.Memo
	for _, memoRelation := range memoRelations {
		memo, err := s.Store.GetMemo(ctx, &store.FindMemo{
			ID: &memoRelation.MemoID,
		})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get memo")
		}
		if memo != nil {
			memoMessage, err := s.convertMemoFromStore(ctx, memo)
			if err != nil {
				return nil, errors.Wrap(err, "failed to convert memo")
			}
			memos = append(memos, memoMessage)
		}
	}

	response := &apiv1.ListMemoCommentsResponse{
		Memos: memos,
	}
	return response, nil
}

func (s *APIV1Service) RenameMemoTag(ctx context.Context, request *apiv1.RenameMemoTagRequest) (*emptypb.Empty, error) {
	user, err := s.GetCurrentUser(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get current user")
	}

	memoFind := &store.FindMemo{
		CreatorID:       &user.ID,
		PayloadFind:     &store.FindMemoPayload{TagSearch: []string{request.OldTag}},
		ExcludeComments: true,
	}
	if (request.Parent) != "memos/-" {
		memoUID, err := ExtractMemoUIDFromName(request.Parent)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid memo name: %v", err)
		}
		memoFind.UID = &memoUID
	}

	memos, err := s.Store.ListMemos(ctx, memoFind)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list memos")
	}

	for _, memo := range memos {
		nodes, err := parser.Parse(tokenizer.Tokenize(memo.Content))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to parse memo: %v", err)
		}
		memopayload.TraverseASTNodes(nodes, func(node ast.Node) {
			if tag, ok := node.(*ast.Tag); ok && tag.Content == request.OldTag {
				tag.Content = request.NewTag
			}
		})
		memo.Content = restore.Restore(nodes)
		if err := memopayload.RebuildMemoPayload(memo); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to rebuild memo payload: %v", err)
		}
		if err := s.Store.UpdateMemo(ctx, &store.UpdateMemo{
			ID:      memo.ID,
			Content: &memo.Content,
			Payload: memo.Payload,
		}); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to update memo: %v", err)
		}
	}

	return &emptypb.Empty{}, nil
}

func (s *APIV1Service) DeleteMemoTag(ctx context.Context, request *apiv1.DeleteMemoTagRequest) (*emptypb.Empty, error) {
	user, err := s.GetCurrentUser(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get current user")
	}

	memoFind := &store.FindMemo{
		CreatorID:       &user.ID,
		PayloadFind:     &store.FindMemoPayload{TagSearch: []string{request.Tag}},
		ExcludeContent:  true,
		ExcludeComments: true,
	}
	if request.Parent != "memos/-" {
		memoUID, err := ExtractMemoUIDFromName(request.Parent)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid memo name: %v", err)
		}
		memoFind.UID = &memoUID
	}

	memos, err := s.Store.ListMemos(ctx, memoFind)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list memos")
	}

	for _, memo := range memos {
		if request.DeleteRelatedMemos {
			err := s.Store.DeleteMemo(ctx, &store.DeleteMemo{ID: memo.ID})
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to delete memo")
			}
		} else {
			archived := store.Archived
			err := s.Store.UpdateMemo(ctx, &store.UpdateMemo{
				ID:        memo.ID,
				RowStatus: &archived,
			})
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to update memo")
			}
		}
	}

	return &emptypb.Empty{}, nil
}

func (s *APIV1Service) getContentLengthLimit(ctx context.Context) (int, error) {
	workspaceMemoRelatedSetting, err := s.Store.GetWorkspaceMemoRelatedSetting(ctx)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "failed to get workspace memo related setting")
	}
	return int(workspaceMemoRelatedSetting.ContentLengthLimit), nil
}

// DispatchMemoCreatedWebhook dispatches webhook when memo is created.
func (s *APIV1Service) DispatchMemoCreatedWebhook(ctx context.Context, memo *apiv1.Memo) error {
	return s.dispatchMemoRelatedWebhook(ctx, memo, "memos.memo.created")
}

// DispatchMemoUpdatedWebhook dispatches webhook when memo is updated.
func (s *APIV1Service) DispatchMemoUpdatedWebhook(ctx context.Context, memo *apiv1.Memo) error {
	return s.dispatchMemoRelatedWebhook(ctx, memo, "memos.memo.updated")
}

// DispatchMemoDeletedWebhook dispatches webhook when memo is deleted.
func (s *APIV1Service) DispatchMemoDeletedWebhook(ctx context.Context, memo *apiv1.Memo) error {
	return s.dispatchMemoRelatedWebhook(ctx, memo, "memos.memo.deleted")
}

func (s *APIV1Service) dispatchMemoRelatedWebhook(ctx context.Context, memo *apiv1.Memo, activityType string) error {
	creatorID, err := ExtractUserIDFromName(memo.Creator)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid memo creator")
	}
	webhooks, err := s.Store.ListWebhooks(ctx, &store.FindWebhook{
		CreatorID: &creatorID,
	})
	if err != nil {
		return err
	}
	for _, hook := range webhooks {
		payload, err := convertMemoToWebhookPayload(memo)
		if err != nil {
			return errors.Wrap(err, "failed to convert memo to webhook payload")
		}
		payload.ActivityType = activityType
		payload.Url = hook.URL
		if err := webhook.Post(payload); err != nil {
			return errors.Wrap(err, "failed to post webhook")
		}
	}
	return nil
}

func convertMemoToWebhookPayload(memo *apiv1.Memo) (*apiv1.WebhookRequestPayload, error) {
	creatorID, err := ExtractUserIDFromName(memo.Creator)
	if err != nil {
		return nil, errors.Wrap(err, "invalid memo creator")
	}
	return &apiv1.WebhookRequestPayload{
		Creator:    fmt.Sprintf("%s%d", UserNamePrefix, creatorID),
		CreateTime: timestamppb.New(time.Now()),
		Memo:       memo,
	}, nil
}

func getMemoContentSnippet(content string) (string, error) {
	nodes, err := parser.Parse(tokenizer.Tokenize(content))
	if err != nil {
		return "", errors.Wrap(err, "failed to parse content")
	}

	plainText := renderer.NewStringRenderer().Render(nodes)
	if len(plainText) > 64 {
		return substring(plainText, 64) + "...", nil
	}
	return plainText, nil
}

func substring(s string, length int) string {
	if length <= 0 {
		return ""
	}

	runeCount := 0
	byteIndex := 0
	for byteIndex < len(s) {
		_, size := utf8.DecodeRuneInString(s[byteIndex:])
		byteIndex += size
		runeCount++
		if runeCount == length {
			break
		}
	}

	return s[:byteIndex]
}

// ExecuteAgentOnMemo implements the gRPC method to trigger an agentic process on a memo.
func (s *APIV1Service) ExecuteAgentOnMemo(ctx context.Context, request *apiv1.ExecuteAgentOnMemoRequest) (*apiv1.Memo, error) {
	currentUser, err := s.GetCurrentUser(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get current user: %v", err)
	}
	if currentUser == nil {
		return nil, status.Errorf(codes.Unauthenticated, "user not authenticated")
	}

	memoUID, err := ExtractMemoUIDFromName(request.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid memo name: %v", err)
	}

	memo, err := s.Store.GetMemo(ctx, &store.FindMemo{UID: &memoUID})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get memo: %v", err)
	}
	if memo == nil {
		return nil, status.Errorf(codes.NotFound, "memo not found: %s", memoUID)
	}

	if memo.CreatorID != currentUser.ID {
		return nil, status.Errorf(codes.PermissionDenied, "user does not have permission to execute agent on this memo")
	}

	agentTaskID := uuid.New().String()
	initialAPIStatus := apiv1.AgentStatus_PENDING
	queryText := request.QueryText
	if queryText == nil || *queryText == "" {
		tempContent := memo.Content
		queryText = &tempContent
	}

	memoUpdate := &store.UpdateMemo{
		ID:          memo.ID,
		AgentTaskID: &agentTaskID,
	}
	storeStatus := convertAgentStatusToStore(initialAPIStatus)
	memoUpdate.AgentStatus = &storeStatus
	memoUpdate.AgentQueryText = queryText

	if err := s.Store.UpdateMemo(ctx, memoUpdate); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update memo with agent task ID: %v", err)
	}

	updatedMemoStore, err := s.Store.GetMemo(ctx, &store.FindMemo{ID: &memo.ID})
	if err != nil {
		slog.Error("failed to get updated memo after setting agent task ID", slog.Any("error", err), slog.String("memo_uid", memoUID))
	} else {
		memo = updatedMemoStore
	}

	deerflowEndpoint := s.Profile.DeerFlowEndpoint
	deerflowAPIKey := s.Profile.DeerFlowAPIKey

	if deerflowEndpoint == "" {
		slog.Error("DeerFlow endpoint is not configured in Memos profile")
		s.updateMemoAgentStatus(ctx, memo.ID, agentTaskID, apiv1.AgentStatus_FAILED, nil, proto.String("DeerFlow endpoint not configured"), nil, nil)
		// Close event channels for this task as it failed immediately.
		s.agentEventChannels.CloseAndRemove(agentTaskID)
		return s.convertMemoFromStore(ctx, memo)
	}

	go s.handleDeerflowSSE(
		context.Background(),
		memo.ID,
		agentTaskID,
		*queryText,
		request,
		deerflowEndpoint,
		deerflowAPIKey,
	)

	slog.Info("Launched Deerflow SSE handler goroutine", "memo_id", memo.ID, "agent_task_id", agentTaskID)

	memoMessage, err := s.convertMemoFromStore(ctx, memo)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to convert updated memo: %v", err)
	}
	return memoMessage, nil
}

// handleDeerflowSSE is run in a goroutine to manage the SSE connection to DeerFlow.
func (s *APIV1Service) handleDeerflowSSE(
	ctx context.Context,
	memoID int32,
	agentTaskID string,
	queryText string,
	agentRequest *apiv1.ExecuteAgentOnMemoRequest,
	deerflowEndpoint string,
	deerflowAPIKey string,
) {
	slog.Info("handleDeerflowSSE started", "memo_id", memoID, "agent_task_id", agentTaskID)
	defer s.agentEventChannels.CloseAndRemove(agentTaskID)
	defer slog.Info("handleDeerflowSSE finished", "memo_id", memoID, "agent_task_id", agentTaskID)

	// 1. Construct DeerFlow ChatRequest
	type DeerFlowChatMessage struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	type DeerFlowChatRequest struct {
		Messages                      []DeerFlowChatMessage `json:"messages"`
		ThreadID                      string                `json:"thread_id"`
		MaxPlanIterations             *int                  `json:"max_plan_iterations,omitempty"`
		MaxStepNum                    *int                  `json:"max_step_num,omitempty"`
		MaxSearchResults              *int                  `json:"max_search_results,omitempty"`
		AutoAcceptedPlan              *bool                 `json:"auto_accepted_plan,omitempty"`
		InterruptFeedback             *string               `json:"interrupt_feedback,omitempty"`
		MCPSettings                   map[string]any        `json:"mcp_settings,omitempty"`
		EnableBackgroundInvestigation *bool                 `json:"enable_background_investigation,omitempty"`
	}

	dfRequest := DeerFlowChatRequest{
		Messages: []DeerFlowChatMessage{{Role: "user", Content: queryText}},
		ThreadID: agentTaskID,
	}

	if agentRequest.MaxPlanIterations != nil {
		val := int(agentRequest.GetMaxPlanIterations())
		dfRequest.MaxPlanIterations = &val
	}
	if agentRequest.MaxStepNum != nil {
		val := int(agentRequest.GetMaxStepNum())
		dfRequest.MaxStepNum = &val
	}
	if agentRequest.MaxSearchResults != nil {
		val := int(agentRequest.GetMaxSearchResults())
		dfRequest.MaxSearchResults = &val
	}
	if agentRequest.AutoAcceptedPlan != nil {
		val := agentRequest.GetAutoAcceptedPlan()
		dfRequest.AutoAcceptedPlan = &val
	}
	if agentRequest.InterruptFeedback != nil {
		val := agentRequest.GetInterruptFeedback()
		dfRequest.InterruptFeedback = &val
	}
	if agentRequest.EnableBackgroundInvestigation != nil {
		val := agentRequest.GetEnableBackgroundInvestigation()
		dfRequest.EnableBackgroundInvestigation = &val
	}
	if agentRequest.McpSettingsOverride != nil && len(agentRequest.GetMcpSettingsOverride()) > 0 {
		dfRequest.MCPSettings = make(map[string]any)
		for k, v := range agentRequest.GetMcpSettingsOverride() {
			dfRequest.MCPSettings[k] = v
		}
	}

	requestBodyBytes, err := json.Marshal(dfRequest)
	if err != nil {
		slog.Error("Failed to marshal DeerFlow request", "error", err, "agent_task_id", agentTaskID)
		s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, apiv1.AgentStatus_FAILED, nil, proto.String(fmt.Sprintf("Failed to prepare request: %v", err)), nil, nil)
		return
	}
	slog.Debug("DeerFlow request body", "body", string(requestBodyBytes))

	req, err := http.NewRequestWithContext(ctx, "POST", deerflowEndpoint, bytes.NewBuffer(requestBodyBytes))
	if err != nil {
		slog.Error("Failed to create DeerFlow HTTP request", "error", err, "agent_task_id", agentTaskID)
		s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, apiv1.AgentStatus_FAILED, nil, proto.String(fmt.Sprintf("Failed to create HTTP request: %v", err)), nil, nil)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	if deerflowAPIKey != "" {
		req.Header.Set("Authorization", "Bearer "+deerflowAPIKey)
	}

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		slog.Error("Failed to connect to DeerFlow", "error", err, "agent_task_id", agentTaskID, "endpoint", deerflowEndpoint)
		s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, apiv1.AgentStatus_FAILED, nil, proto.String(fmt.Sprintf("Connection to DeerFlow failed: %v", err)), nil, nil)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errorBody []byte
		if resp.Body != nil {
			errorBody, _ = io.ReadAll(resp.Body)
		}
		slog.Error("DeerFlow request failed", "status_code", resp.StatusCode, "body", string(errorBody))
		s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, apiv1.AgentStatus_FAILED, nil, proto.String(fmt.Sprintf("DeerFlow error %d: %s", resp.StatusCode, string(errorBody))), nil, nil)
		return
	}
	slog.Info("Connected to DeerFlow SSE stream", "agent_task_id", agentTaskID)

	reader := bufio.NewReader(resp.Body)
	var eventTypeStr, dataContent, sseID string
	var resultBuilder strings.Builder
	var planJSONString string
	var stepsJSONArray []string
	var accumulatedStepsJson string = "[]"

	type DeerFlowMessageChunkData struct {
		ThreadID     string  `json:"thread_id"`
		Agent        string  `json:"agent"`
		ID           string  `json:"id"`
		Role         string  `json:"role"`
		Content      string  `json:"content"`
		FinishReason *string `json:"finish_reason,omitempty"`
	}
	type DeerFlowPlanData struct {
		Steps []struct {
			Title       string `json:"title"`
			Description string `json:"description"`
			Agent       string `json:"agent"`
		} `json:"steps"`
		RawPlanText string `json:"raw_plan_text"`
	}
	type DeerFlowThoughtData struct {
		Thought string `json:"thought"`
		Agent   string `json:"agent"`
	}
	type DeerFlowToolCallData struct {
		ID       string `json:"id"`
		Type     string `json:"type"`
		Function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
		Agent string `json:"agent"`
	}
	type DeerFlowToolResultData struct {
		ToolCallID string `json:"tool_call_id"`
		Output     any    `json:"output"`
		Agent      string `json:"agent"`
		IsError    bool   `json:"is_error,omitempty"`
	}
	type DeerFlowInterruptData struct {
		Reason           string         `json:"reason"`
		FeedbackRequired bool           `json:"feedback_required"`
		State            map[string]any `json:"state,omitempty"`
		Agent            string         `json:"agent"`
	}

	updateStatusAndNotify := func(newApiStatus apiv1.AgentStatus, dataForEvent *string, isError bool, errorMessage string, eventTypeForNotification string) {
		var dataJsonToSend string
		if dataForEvent != nil {
			dataJsonToSend = *dataForEvent
		} else if isError && errorMessage != "" {
			escapedError := strings.ReplaceAll(errorMessage, "\"", "\\\"")
			escapedError = strings.ReplaceAll(escapedError, "\n", "\\n")
			dataJsonToSend = fmt.Sprintf("{\"error\": \"%s\"}", escapedError)
		} else {
			dataJsonToSend = "{}"
		}

		var sseIDForEvent *string
		if sseID != "" {
			tempID := sseID
			sseIDForEvent = &tempID
		}

		statusSnapshot := newApiStatus
		apiEvent := &apiv1.AgentTaskEvent{
			EventType:          eventTypeForNotification,
			DataJson:           dataJsonToSend,
			Timestamp:          timestamppb.Now(),
			IsError:            isError,
			ErrorMessage:       proto.String(errorMessage),
			CurrentAgentStatus: &statusSnapshot,
			SourceEventId:      sseIDForEvent,
		}
		s.agentEventChannels.Send(agentTaskID, apiEvent)
	}

	updateStatusAndNotify(apiv1.AgentStatus_EXECUTING, nil, false, "", "STATUS_UPDATE")

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				slog.Info("DeerFlow SSE stream ended (EOF)", "agent_task_id", agentTaskID)
				currentMemo, getErr := s.Store.GetMemo(context.Background(), &store.FindMemo{ID: &memoID})
				finalStatusOnEOF := apiv1.AgentStatus_COMPLETED
				var finalMsgOnEOF string

				if getErr == nil && currentMemo != nil && currentMemo.AgentStatus != nil {
					currentDBStatus := convertStoreAgentStatusToAPI(*currentMemo.AgentStatus)
					if currentDBStatus == apiv1.AgentStatus_FAILED || currentDBStatus == apiv1.AgentStatus_AGENT_STATUS_INTERRUPTED || currentDBStatus == apiv1.AgentStatus_CANCELED {
						finalStatusOnEOF = currentDBStatus
						if currentMemo.AgentErrorMessage != nil {
							finalMsgOnEOF = *currentMemo.AgentErrorMessage
						} else if currentMemo.AgentResultJson != nil {
							finalMsgOnEOF = *currentMemo.AgentResultJson
						}
					} else if currentDBStatus != apiv1.AgentStatus_COMPLETED {
						slog.Warn("SSE stream ended (EOF) while memo was not in a final state. Marking as COMPLETED.", "agent_task_id", agentTaskID, "db_status", currentDBStatus.String())
						finalMsgOnEOF = resultBuilder.String()
						if finalMsgOnEOF == "" {
							finalMsgOnEOF = "{ \"info\": \"Task ended, no specific result accumulated via message_chunk.\" }"
						}
					}
				} else if getErr != nil {
					slog.Error("Failed to get memo status on EOF, defaulting to COMPLETED", "error", getErr, "agent_task_id", agentTaskID)
					finalMsgOnEOF = resultBuilder.String()
					if finalMsgOnEOF == "" {
						finalMsgOnEOF = "{ \"info\": \"Task ended, no specific result accumulated, and current state unknown.\" }"
					}
				}
				s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, finalStatusOnEOF, &finalMsgOnEOF, nil, &planJSONString, &accumulatedStepsJson)
				updateStatusAndNotify(finalStatusOnEOF, &finalMsgOnEOF, finalStatusOnEOF == apiv1.AgentStatus_FAILED, "Stream ended", "TASK_FINALIZED")
			} else {
				slog.Error("Error reading from DeerFlow SSE stream", "error", err, "agent_task_id", agentTaskID)
				errMsg := fmt.Sprintf("Stream read error: %v", err)
				s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, apiv1.AgentStatus_FAILED, nil, &errMsg, &planJSONString, &accumulatedStepsJson)
				updateStatusAndNotify(apiv1.AgentStatus_FAILED, nil, true, errMsg, "TASK_FAILED")
			}
			return
		}

		line = strings.TrimSpace(line)

		if line == "" {
			if dataContent != "" {
				slog.Debug("DeerFlow SSE Event Received", "id", sseID, "type", eventTypeStr, "data", dataContent, "agent_task_id", agentTaskID)

				var currentStatusForEvent apiv1.AgentStatus = apiv1.AgentStatus_EXECUTING
				var isErrorPayload bool = false
				var errorMessageInPayload string
				var dataToSendToClient string = dataContent
				memosEventType := eventTypeStr

				switch eventTypeStr {
				case "message_chunk":
					var chunkData DeerFlowMessageChunkData
					if err := json.Unmarshal([]byte(dataContent), &chunkData); err == nil {
						resultBuilder.WriteString(chunkData.Content)
					} else {
						slog.Warn("Failed to unmarshal message_chunk data", "error", err, "data", dataContent, "agent_task_id", agentTaskID)
					}
					memosEventType = "MESSAGE_CHUNK"
					currentStatusForEvent = apiv1.AgentStatus_EXECUTING
				case "plan":
					var planData DeerFlowPlanData
					if err := json.Unmarshal([]byte(dataContent), &planData); err == nil {
						planJSONBytes, _ := json.Marshal(planData)
						planJSONString = string(planJSONBytes)
						s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, apiv1.AgentStatus_PLANNING, nil, nil, &planJSONString, &accumulatedStepsJson)
					} else {
						slog.Warn("Failed to unmarshal plan data", "error", err, "data", dataContent, "agent_task_id", agentTaskID)
						planJSONString = dataContent
					}
					memosEventType = "PLAN_GENERATED"
					currentStatusForEvent = apiv1.AgentStatus_PLANNING
				case "thought":
					memosEventType = "AGENT_THOUGHT"
					currentStatusForEvent = apiv1.AgentStatus_EXECUTING
				case "tool_call":
					var toolCallData DeerFlowToolCallData
					if err := json.Unmarshal([]byte(dataContent), &toolCallData); err == nil {
						stepsJSONArray = append(stepsJSONArray, dataContent)
						stepsBytes, _ := json.Marshal(stepsJSONArray)
						accumulatedStepsJson = string(stepsBytes)
						s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, apiv1.AgentStatus_EXECUTING, nil, nil, &planJSONString, &accumulatedStepsJson)
					} else {
						slog.Warn("Failed to unmarshal tool_call data", "error", err, "data", dataContent, "agent_task_id", agentTaskID)
					}
					memosEventType = "TOOL_CALL_REQUESTED"
					currentStatusForEvent = apiv1.AgentStatus_EXECUTING
				case "tool_result":
					var toolResultData DeerFlowToolResultData
					if err := json.Unmarshal([]byte(dataContent), &toolResultData); err == nil {
						stepsJSONArray = append(stepsJSONArray, dataContent)
						stepsBytes, _ := json.Marshal(stepsJSONArray)
						accumulatedStepsJson = string(stepsBytes)
						s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, apiv1.AgentStatus_EXECUTING, nil, nil, &planJSONString, &accumulatedStepsJson)
						if toolResultData.IsError {
							isErrorPayload = true
							if outputBytes, ok := toolResultData.Output.([]byte); ok {
								errorMessageInPayload = string(outputBytes)
							} else if outputStr, ok := toolResultData.Output.(string); ok {
								errorMessageInPayload = outputStr
							} else {
								jsonOutput, _ := json.Marshal(toolResultData.Output)
								errorMessageInPayload = string(jsonOutput)
							}
						}
					} else {
						slog.Warn("Failed to unmarshal tool_result data", "error", err, "data", dataContent, "agent_task_id", agentTaskID)
					}
					memosEventType = "TOOL_CALL_COMPLETED"
					currentStatusForEvent = apiv1.AgentStatus_EXECUTING
				case "interrupt":
					memosEventType = "USER_INTERRUPT_REQUIRED"
					currentStatusForEvent = apiv1.AgentStatus_AGENT_STATUS_INTERRUPTED
					s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, currentStatusForEvent, nil, nil, &planJSONString, &accumulatedStepsJson)
				case "task_completed":
					finalResult := resultBuilder.String()
					var deerflowResult struct {
						Result any `json:"result"`
					}
					if err := json.Unmarshal([]byte(dataContent), &deerflowResult); err == nil && deerflowResult.Result != nil {
						if resStr, ok := deerflowResult.Result.(string); ok {
							finalResult = resStr
						} else {
							resBytes, _ := json.Marshal(deerflowResult.Result)
							finalResult = string(resBytes)
						}
					}
					memosEventType = "TASK_COMPLETED"
					currentStatusForEvent = apiv1.AgentStatus_COMPLETED
					s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, currentStatusForEvent, &finalResult, nil, &planJSONString, &accumulatedStepsJson)
					dataToSendToClient = fmt.Sprintf("{\"result\": \"%s\"}", strings.ReplaceAll(finalResult, "\"", "\\\""))
				case "task_failed":
					var deerflowError struct {
						Error string `json:"error"`
					}
					if err := json.Unmarshal([]byte(dataContent), &deerflowError); err == nil && deerflowError.Error != "" {
						errorMessageInPayload = deerflowError.Error
					} else {
						errorMessageInPayload = dataContent
					}
					isErrorPayload = true
					memosEventType = "TASK_FAILED"
					currentStatusForEvent = apiv1.AgentStatus_FAILED
					s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, currentStatusForEvent, nil, &errorMessageInPayload, &planJSONString, &accumulatedStepsJson)
					dataToSendToClient = fmt.Sprintf("{\"error\": \"%s\"}", strings.ReplaceAll(errorMessageInPayload, "\"", "\\\""))
				default:
					slog.Debug("Unknown or unhandled DeerFlow event type, forwarding as generic", "type", eventTypeStr, "data", dataContent, "agent_task_id", agentTaskID)
					memosEventType = "UNKNOWN_" + strings.ToUpper(eventTypeStr)
				}

				updateStatusAndNotify(currentStatusForEvent, &dataToSendToClient, isErrorPayload, errorMessageInPayload, memosEventType)

				dataContent = ""
				eventTypeStr = ""
				sseID = ""
			}
		} else if strings.HasPrefix(line, "event:") {
			eventTypeStr = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			if dataContent != "" {
				dataContent += "\n"
			}
			dataContent += strings.TrimPrefix(line, "data:")
		} else if strings.HasPrefix(line, "id:") {
			sseID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
		}
	}
}

// StreamAgentTaskEvents is a server-streaming RPC that sends AgentTaskEvents to the client.
func (s *APIV1Service) StreamAgentTaskEvents(request *apiv1.StreamAgentTaskEventsRequest, stream apiv1.MemoService_StreamAgentTaskEventsServer) error {
	agentTaskID := request.AgentTaskId
	if agentTaskID == "" {
		return status.Errorf(codes.InvalidArgument, "agent_task_id is required")
	}

	slog.Info("Client subscribed to agent events", "agent_task_id", agentTaskID)
	eventChan := make(chan *apiv1.AgentTaskEvent, 100)

	s.agentEventChannels.Register(agentTaskID, eventChan)
	defer func() {
		s.agentEventChannels.Unregister(agentTaskID, eventChan)
		slog.Info("Client unsubscribed from agent events", "agent_task_id", agentTaskID)
	}()

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			slog.Info("Client connection closed or context cancelled for agent events", "agent_task_id", agentTaskID, "error", ctx.Err())
			return ctx.Err()
		case event, ok := <-eventChan:
			if !ok {
				slog.Info("Agent event channel closed by producer (task likely finished)", "agent_task_id", agentTaskID)
				return nil
			}
			if err := stream.Send(event); err != nil {
				slog.Error("Failed to send agent event to client", "error", err, "agent_task_id", agentTaskID)
				return err
			}
			slog.Debug("Sent agent event to client via stream", "agent_task_id", agentTaskID, "event_type", event.EventType)
		}
	}
}

// updateMemoAgentStatus is a helper to update the agent-related fields of a memo.
func (s *APIV1Service) updateMemoAgentStatus(
	ctx context.Context,
	memoID int32,
	agentTaskID string,
	status apiv1.AgentStatus,
	resultJSON *string,
	errorMessage *string,
	planJSON *string,
	stepsJSON *string,
) {
	update := &store.UpdateMemo{
		ID: memoID,
	}
	storeStatus := convertAgentStatusToStore(status)
	update.AgentStatus = &storeStatus

	if resultJSON != nil {
		update.AgentResultJson = resultJSON
	}
	if errorMessage != nil {
		update.AgentErrorMessage = errorMessage
	}
	if planJSON != nil {
		update.AgentPlanJson = planJSON
	}
	if stepsJSON != nil {
		update.AgentStepsJson = stepsJSON
	}

	if err := s.Store.UpdateMemo(ctx, update); err != nil {
		slog.Error("Failed to update memo agent status in DB",
			"error", err, "memo_id", memoID, "agent_task_id", agentTaskID,
			"target_status", status.String(), "result_len", lenPtr(resultJSON),
			"error_len", lenPtr(errorMessage), "plan_len", lenPtr(planJSON), "steps_len", lenPtr(stepsJSON))
	} else {
		slog.Info("Memo agent status updated in DB",
			"memo_id", memoID, "agent_task_id", agentTaskID,
			"target_status", status.String(), "result_len", lenPtr(resultJSON),
			"error_len", lenPtr(errorMessage), "plan_len", lenPtr(planJSON), "steps_len", lenPtr(stepsJSON))
	}
}

func lenPtr(s *string) int {
	if s == nil {
		return 0
	}
	return len(*s)
}

func convertStoreAgentStatusToAPI(storeStatusValue int32) apiv1.AgentStatus {
	return apiv1.AgentStatus(storeStatusValue)
}
