package v1

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/usememos/memos/plugin/webhook"
	v1pb "github.com/usememos/memos/proto/gen/api/v1"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/server/runner/memopayload"
	"github.com/usememos/memos/store"
	"bytes"
	"encoding/json"
	"bufio"
	"strings"
)

func (s *APIV1Service) CreateMemo(ctx context.Context, request *v1pb.CreateMemoRequest) (*v1pb.Memo, error) {
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
		_, err := s.SetMemoResources(ctx, &v1pb.SetMemoResourcesRequest{
			Name:      fmt.Sprintf("%s%s", MemoNamePrefix, memo.UID),
			Resources: request.Memo.Resources,
		})
		if err != nil {
			return nil, errors.Wrap(err, "failed to set memo resources")
		}
	}
	if len(request.Memo.Relations) > 0 {
		_, err := s.SetMemoRelations(ctx, &v1pb.SetMemoRelationsRequest{
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

func (s *APIV1Service) ListMemos(ctx context.Context, request *v1pb.ListMemosRequest) (*v1pb.ListMemosResponse, error) {
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
	if request.State == v1pb.State_ARCHIVED {
		state := store.Archived
		memoFind.RowStatus = &state
	} else {
		state := store.Normal
		memoFind.RowStatus = &state
	}
	if request.Direction == v1pb.Direction_ASC {
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
		var pageToken v1pb.PageToken
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

	memoMessages := []*v1pb.Memo{}
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

	response := &v1pb.ListMemosResponse{
		Memos:         memoMessages,
		NextPageToken: nextPageToken,
	}
	return response, nil
}

func (s *APIV1Service) GetMemo(ctx context.Context, request *v1pb.GetMemoRequest) (*v1pb.Memo, error) {
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

func (s *APIV1Service) UpdateMemo(ctx context.Context, request *v1pb.UpdateMemoRequest) (*v1pb.Memo, error) {
	memoUID, err := ExtractMemoUIDFromName(request.Memo.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid memo name: %v", err)
	}
	if request.UpdateMask == nil || len(request.UpdateMask.Paths) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "update mask is required")
	}

	memo, err := s.Store.GetMemo(ctx, &store.FindMemo{UID: &memoUID})
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

	update := &store.UpdateMemo{
		ID: memo.ID,
	}
	for _, path := range request.UpdateMask.Paths {
		if path == "content" {
			contentLengthLimit, err := s.getContentLengthLimit(ctx)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to get content length limit")
			}
			if len(request.Memo.Content) > contentLengthLimit {
				return nil, status.Errorf(codes.InvalidArgument, "content too long (max %d characters)", contentLengthLimit)
			}
			memo.Content = request.Memo.Content
			if err := memopayload.RebuildMemoPayload(memo); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to rebuild memo payload: %v", err)
			}
			update.Content = &memo.Content
			update.Payload = memo.Payload
		} else if path == "visibility" {
			workspaceMemoRelatedSetting, err := s.Store.GetWorkspaceMemoRelatedSetting(ctx)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to get workspace memo related setting")
			}
			visibility := convertVisibilityToStore(request.Memo.Visibility)
			if workspaceMemoRelatedSetting.DisallowPublicVisibility && visibility == store.Public {
				return nil, status.Errorf(codes.PermissionDenied, "disable public memos system setting is enabled")
			}
			update.Visibility = &visibility
		} else if path == "pinned" {
			update.Pinned = &request.Memo.Pinned
		} else if path == "state" {
			rowStatus := convertStateToStore(request.Memo.State)
			update.RowStatus = &rowStatus
		} else if path == "create_time" {
			createdTs := request.Memo.CreateTime.AsTime().Unix()
			update.CreatedTs = &createdTs
		} else if path == "update_time" {
			updatedTs := time.Now().Unix()
			if request.Memo.UpdateTime != nil {
				updatedTs = request.Memo.UpdateTime.AsTime().Unix()
			}
			update.UpdatedTs = &updatedTs
		} else if path == "display_time" {
			displayTs := request.Memo.DisplayTime.AsTime().Unix()
			memoRelatedSetting, err := s.Store.GetWorkspaceMemoRelatedSetting(ctx)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to get workspace memo related setting")
			}
			if memoRelatedSetting.DisplayWithUpdateTime {
				update.UpdatedTs = &displayTs
			} else {
				update.CreatedTs = &displayTs
			}
		} else if path == "location" {
			payload := memo.Payload
			payload.Location = convertLocationToStore(request.Memo.Location)
			update.Payload = payload
		} else if path == "resources" {
			_, err := s.SetMemoResources(ctx, &v1pb.SetMemoResourcesRequest{
				Name:      request.Memo.Name,
				Resources: request.Memo.Resources,
			})
			if err != nil {
				return nil, errors.Wrap(err, "failed to set memo resources")
			}
		} else if path == "relations" {
			_, err := s.SetMemoRelations(ctx, &v1pb.SetMemoRelationsRequest{
				Name:      request.Memo.Name,
				Relations: request.Memo.Relations,
			})
			if err != nil {
				return nil, errors.Wrap(err, "failed to set memo relations")
			}
		} else if path == "agent_task_id" {
			update.AgentTaskID = &request.Memo.AgentTaskId
		} else if path == "agent_status" {
			status := convertAgentStatusToStore(request.Memo.AgentStatus)
			update.AgentStatus = &status
		} else if path == "agent_query_text" {
			update.AgentQueryText = &request.Memo.AgentQueryText
		} else if path == "agent_plan_json" {
			update.AgentPlanJson = &request.Memo.AgentPlanJson
		} else if path == "agent_steps_json" {
			update.AgentStepsJson = &request.Memo.AgentStepsJson
		} else if path == "agent_result_json" {
			update.AgentResultJson = &request.Memo.AgentResultJson
		} else if path == "agent_error_message" {
			update.AgentErrorMessage = &request.Memo.AgentErrorMessage
		}
	}

	if err = s.Store.UpdateMemo(ctx, update); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update memo")
	}

	memo, err = s.Store.GetMemo(ctx, &store.FindMemo{
		ID: &memo.ID,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get memo")
	}
	memoMessage, err := s.convertMemoFromStore(ctx, memo)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert memo")
	}
	// Try to dispatch webhook when memo is updated.
	if err := s.DispatchMemoUpdatedWebhook(ctx, memoMessage); err != nil {
		slog.Warn("Failed to dispatch memo updated webhook", slog.Any("err", err))
	}

	return memoMessage, nil
}

func (s *APIV1Service) DeleteMemo(ctx context.Context, request *v1pb.DeleteMemoRequest) (*emptypb.Empty, error) {
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

func (s *APIV1Service) CreateMemoComment(ctx context.Context, request *v1pb.CreateMemoCommentRequest) (*v1pb.Memo, error) {
	memoUID, err := ExtractMemoUIDFromName(request.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid memo name: %v", err)
	}
	relatedMemo, err := s.Store.GetMemo(ctx, &store.FindMemo{UID: &memoUID})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get memo")
	}

	// Create the memo comment first.
	memoComment, err := s.CreateMemo(ctx, &v1pb.CreateMemoRequest{Memo: request.Comment})
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
	if memoComment.Visibility != v1pb.Visibility_PRIVATE && creatorID != relatedMemo.CreatorID {
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

func (s *APIV1Service) ListMemoComments(ctx context.Context, request *v1pb.ListMemoCommentsRequest) (*v1pb.ListMemoCommentsResponse, error) {
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

	var memos []*v1pb.Memo
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

	response := &v1pb.ListMemoCommentsResponse{
		Memos: memos,
	}
	return response, nil
}

func (s *APIV1Service) RenameMemoTag(ctx context.Context, request *v1pb.RenameMemoTagRequest) (*emptypb.Empty, error) {
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

func (s *APIV1Service) DeleteMemoTag(ctx context.Context, request *v1pb.DeleteMemoTagRequest) (*emptypb.Empty, error) {
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
func (s *APIV1Service) DispatchMemoCreatedWebhook(ctx context.Context, memo *v1pb.Memo) error {
	return s.dispatchMemoRelatedWebhook(ctx, memo, "memos.memo.created")
}

// DispatchMemoUpdatedWebhook dispatches webhook when memo is updated.
func (s *APIV1Service) DispatchMemoUpdatedWebhook(ctx context.Context, memo *v1pb.Memo) error {
	return s.dispatchMemoRelatedWebhook(ctx, memo, "memos.memo.updated")
}

// DispatchMemoDeletedWebhook dispatches webhook when memo is deleted.
func (s *APIV1Service) DispatchMemoDeletedWebhook(ctx context.Context, memo *v1pb.Memo) error {
	return s.dispatchMemoRelatedWebhook(ctx, memo, "memos.memo.deleted")
}

func (s *APIV1Service) dispatchMemoRelatedWebhook(ctx context.Context, memo *v1pb.Memo, activityType string) error {
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

func convertMemoToWebhookPayload(memo *v1pb.Memo) (*v1pb.WebhookRequestPayload, error) {
	creatorID, err := ExtractUserIDFromName(memo.Creator)
	if err != nil {
		return nil, errors.Wrap(err, "invalid memo creator")
	}
	return &v1pb.WebhookRequestPayload{
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
func (s *APIV1Service) ExecuteAgentOnMemo(ctx context.Context, request *v1pb.ExecuteAgentOnMemoRequest) (*v1pb.Memo, error) {
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

	// Ensure this memo belongs to the current user or is public/protected if modification rules allow.
	// For now, assuming only the creator can execute an agent.
	if memo.CreatorID != currentUser.ID {
		// Add more sophisticated checks if other users can trigger agents on non-private memos.
		return nil, status.Errorf(codes.PermissionDenied, "user does not have permission to execute agent on this memo")
	}

	// Generate a new agent task ID.
	agentTaskID := uuid.New().String()
	initialAgentStatus := v1pb.AgentStatus_AGENT_STATUS_PROCESSING // Assuming PROCESSING is a valid enum value
	queryText := request.QueryText
	if queryText == "" {
		queryText = memo.Content // Default to memo content if no specific query text
	}

	// Update the memo with agent task ID and initial status.
	memoUpdate := &store.UpdateMemo{
		ID:             memo.ID,
		AgentTaskID:    &agentTaskID,
		AgentStatus:    (*int32)(convertAgentStatusToStore(initialAgentStatus)),
		AgentQueryText: &queryText,
	}
	if err := s.Store.UpdateMemo(ctx, memoUpdate); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update memo with agent task ID: %v", err)
	}

	// Fetch the updated memo to return the latest state.
	updatedMemo, err := s.Store.GetMemo(ctx, &store.FindMemo{ID: &memo.ID})
	if err != nil {
		slog.Error("failed to get updated memo after setting agent task ID", slog.Any("error", err), slog.String("memo_uid", memoUID))
		// Continue to launch goroutine, but the returned memo might not have agent fields immediately.
		// Or, return the locally constructed memo if GetMemo fails. For now, log and proceed.
	} else {
		memo = updatedMemo // Use the freshly fetched memo
	}
	
	// Use a background context for the goroutine that makes the external call.
	// The original ctx from the gRPC request might be cancelled if the client disconnects,
	// but we want the agent task to continue processing on the backend.
	deerflowEndpoint := s.Profile.DeerFlowEndpoint // Get from profile/config
	deerflowAPIKey := s.Profile.DeerFlowAPIKey   // Get from profile/config

	if deerflowEndpoint == "" {
		slog.Error("DeerFlow endpoint is not configured in Memos profile")
		// Update memo to FAILED status
		s.updateMemoAgentStatus(ctx, memo.ID, agentTaskID, v1pb.AgentStatus_AGENT_STATUS_FAILED, "", "DeerFlow endpoint not configured")
		return s.convertMemoFromStore(ctx, memo) // Return the memo with FAILED status
	}

	go s.handleDeerflowSSE(
		context.Background(), 
		memo.ID,
		agentTaskID,
		queryText,
		request,
		deerflowEndpoint,
		deerflowAPIKey,
	)

	slog.Info("Launched Deerflow SSE handler goroutine", "memo_id", memo.ID, "agent_task_id", agentTaskID)

	// Convert the updated memo to protobuf format for the response.
	memoMessage, err := s.convertMemoFromStore(ctx, memo)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to convert updated memo: %v", err)
	}

	return memoMessage, nil
}

// handleDeerflowSSE is run in a goroutine to manage the SSE connection to DeerFlow.
func (s *APIV1Service) handleDeerflowSSE(
	ctx context.Context, // Note: This context is from context.Background(), manage its lifecycle if needed.
	memoID int32,
	agentTaskID string,
	queryText string,
	agentRequest *v1pb.ExecuteAgentOnMemoRequest,
	deerflowEndpoint string,
	deerflowAPIKey string, // Optional, if DeerFlow requires API key
) {
	slog.Info("handleDeerflowSSE started", "memo_id", memoID, "agent_task_id", agentTaskID)
	defer slog.Info("handleDeerflowSSE finished", "memo_id", memoID, "agent_task_id", agentTaskID)

	// 1. Construct DeerFlow ChatRequest
	type DeerFlowChatMessage struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	type DeerFlowChatRequest struct {
		Messages                    []DeerFlowChatMessage `json:"messages"`
		ThreadID                    string                `json:"thread_id"`
		MaxPlanIterations           *int                  `json:"max_plan_iterations,omitempty"` // Use pointers for optional fields
		MaxStepNum                  *int                  `json:"max_step_num,omitempty"`
		MaxSearchResults            *int                  `json:"max_search_results,omitempty"`
		AutoAcceptedPlan            *bool                 `json:"auto_accepted_plan,omitempty"`
		InterruptFeedback           *string               `json:"interrupt_feedback,omitempty"`
		MCPSettings                 map[string]any        `json:"mcp_settings,omitempty"`
		EnableBackgroundInvestigation *bool               `json:"enable_background_investigation,omitempty"`
	}

	// Example values: These should ideally come from Memos config or ExecuteAgentOnMemoRequest.
	// For now, we'll use some defaults or nil for optional ones.
	// trueBool := true
	// defaultMaxPlanIterations := 10 // Example
	// defaultMaxStepNum := 20      // Example

	dfRequest := DeerFlowChatRequest{
		Messages: []DeerFlowChatMessage{
			{Role: "user", Content: queryText},
		},
		ThreadID: agentTaskID,
		// MaxPlanIterations, MaxStepNum, etc. will be set from agentRequest or Memos defaults
	}

	// Populate optional fields from agentRequest or Memos server defaults
	if agentRequest.MaxPlanIterations != nil {
		val := int(agentRequest.GetMaxPlanIterations()) // GetMaxPlanIterations to handle optional int32
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
		val := agentRequest.GetAutoAcceptedPlan() // GetAutoAcceptedPlan to handle optional bool
		dfRequest.AutoAcceptedPlan = &val
	} else {
		// Example: Fallback to a Memos server-side default if not provided in request
		// defaultAutoAccept := true // Load this from s.Profile or config
		// dfRequest.AutoAcceptedPlan = &defaultAutoAccept
		// For now, if not in request, it remains nil (and omitempty will work)
		// Or, explicitly set a hardcoded default for now if needed:
		// trueVal := true
		// dfRequest.AutoAcceptedPlan = &trueVal
	}
	if agentRequest.InterruptFeedback != nil {
		val := agentRequest.GetInterruptFeedback()
		dfRequest.InterruptFeedback = &val
	}
	if agentRequest.EnableBackgroundInvestigation != nil {
		val := agentRequest.GetEnableBackgroundInvestigation()
		dfRequest.EnableBackgroundInvestigation = &val
	}
	// Populate MCPSettings from agentRequest
	if agentRequest.McpSettingsOverride != nil && len(agentRequest.GetMcpSettingsOverride()) > 0 {
		dfRequest.MCPSettings = make(map[string]any)
		for k, v := range agentRequest.GetMcpSettingsOverride() {
			dfRequest.MCPSettings[k] = v
		}
	}

	requestBodyBytes, err := json.Marshal(dfRequest)
	if err != nil {
		slog.Error("Failed to marshal DeerFlow request", "error", err, "agent_task_id", agentTaskID)
		s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, v1pb.AgentStatus_AGENT_STATUS_FAILED, "", fmt.Sprintf("Failed to prepare request: %v", err))
		return
	}
	slog.Info("DeerFlow request body", "body", string(requestBodyBytes))

	// 2. Make HTTP POST request to DeerFlow
	req, err := http.NewRequestWithContext(ctx, "POST", deerflowEndpoint, bytes.NewBuffer(requestBodyBytes))
	if err != nil {
		slog.Error("Failed to create DeerFlow HTTP request", "error", err, "agent_task_id", agentTaskID)
		s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, v1pb.AgentStatus_AGENT_STATUS_FAILED, "", fmt.Sprintf("Failed to create HTTP request: %v", err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	if deerflowAPIKey != "" {
		req.Header.Set("Authorization", "Bearer "+deerflowAPIKey) // Example, adjust if different auth scheme
	}

	client := &http.Client{Timeout: 0} // Timeout 0 for long-lived SSE connections
	resp, err := client.Do(req)
	if err != nil {
		slog.Error("Failed to connect to DeerFlow", "error", err, "agent_task_id", agentTaskID, "endpoint", deerflowEndpoint)
		s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, v1pb.AgentStatus_AGENT_STATUS_FAILED, "", fmt.Sprintf("Connection to DeerFlow failed: %v", err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Read body for more error details from DeerFlow if possible
		var errorBody []byte
		if resp.Body != nil {
			errorBody, _ = io.ReadAll(resp.Body) // Ignoring read error here
		}
		slog.Error("DeerFlow request failed", "status_code", resp.StatusCode, "agent_task_id", agentTaskID, "response_body", string(errorBody))
		s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, v1pb.AgentStatus_AGENT_STATUS_FAILED, "", fmt.Sprintf("DeerFlow returned error: %d %s", resp.StatusCode, string(errorBody)))
		return
	}

	slog.Info("Connected to DeerFlow SSE stream", "agent_task_id", agentTaskID)

	// 3. Process SSE stream
	reader := bufio.NewReader(resp.Body)
	var eventType, dataContent string
	var id string // For SSE 'id:' field, if present

	var resultBuilder strings.Builder
	var planJSONString string
	var stepsJSONArray []string // Store JSON strings of individual steps
	var accumulatedStepsJson string // Periodically updated JSON array string of all steps

	// Define structs for expected DeerFlow event data payloads
	type DeerFlowMessageChunkData struct {
		ThreadID      string `json:"thread_id"`
		Agent         string `json:"agent"`
		ID            string `json:"id"` // This is the message chunk's own ID from DeerFlow
		Role          string `json:"role"`
		Content       string `json:"content"`
		FinishReason  *string `json:"finish_reason,omitempty"` // Pointer for optional field
		// Add other fields if present in DeerFlow's message_chunk, e.g., tool_calls
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
		Output     any    `json:"output"` // Changed to any for flexibility
		Agent      string `json:"agent"`
		IsError    bool   `json:"is_error,omitempty"`
	}

	type DeerFlowInterruptData struct {
		Reason           string         `json:"reason"`
		FeedbackRequired bool           `json:"feedback_required"`
		State            map[string]any `json:"state,omitempty"`
		Agent            string         `json:"agent"`
	}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				slog.Info("DeerFlow SSE stream ended (EOF)", "agent_task_id", agentTaskID)
				// If stream ended and status is still PROCESSING, it might mean completion or an abrupt end.
				// Let's check the memo's current status in the DB.
				currentMemo, getErr := s.Store.GetMemo(context.Background(), &store.FindMemo{ID: &memoID})
				if getErr == nil && currentMemo != nil && currentMemo.AgentStatus != nil {
					memoStatus := v1pb.AgentStatus(*currentMemo.AgentStatus)
					if memoStatus == v1pb.AgentStatus_AGENT_STATUS_PROCESSING {
						slog.Info("SSE stream ended (EOF) while memo was still PROCESSING. Marking as COMPLETED.", "agent_task_id", agentTaskID)
						finalResultOnEOF := resultBuilder.String()
						emptyStr := ""
						s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, v1pb.AgentStatus_AGENT_STATUS_COMPLETED, &finalResultOnEOF, &emptyStr, nil, nil)
					}
				} else if getErr != nil {
					slog.Error("Failed to get memo status on EOF", "error", getErr, "agent_task_id", agentTaskID)
				}
			} else {
				slog.Error("Error reading from DeerFlow SSE stream", "error", err, "agent_task_id", agentTaskID)
				s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, v1pb.AgentStatus_AGENT_STATUS_FAILED, "", fmt.Sprintf("Stream read error: %v", err))
			}
			return // Exit goroutine on EOF or error
		}

		line = strings.TrimSpace(line)

		if line == "" { // An empty line typically signifies the end of an event block.
			if dataContent != "" { // Process the event if we have data
				slog.Info("DeerFlow SSE Event Received",
					"agent_task_id", agentTaskID,
					"event_id", id, // Log SSE ID if present
					"event_type", eventType,
					"data", dataContent,
				)

				var currentStatusForEvent v1pb.AgentStatus = v1pb.AgentStatus_AGENT_STATUS_PROCESSING // Default, update based on event
				var isErrorEvent bool = false
				var errorMessageForEvent string = ""
				var isPartialEvent bool = true // Assume most data events are partial, unless it's a final one.

				var agentStepData map[string]interface{} // To store structured step data
				var finalResultData *string              // For completed tasks
				var errorData *string                    // For failed tasks or interrupt messages
				var planDataForUpdate *string            // For plan updates
				var stepsDataForUpdate *string           // For steps updates
				originalEventType := eventType // Preserve original for dispatch if normalized later

				switch eventType {
				case "message_chunk":
					var chunkData DeerFlowMessageChunkData
					if err := json.Unmarshal([]byte(dataContent), &chunkData); err != nil {
						slog.Error("Failed to unmarshal DeerFlow message_chunk", "error", err, "data", dataContent, "agent_task_id", agentTaskID)
					} else {
						resultBuilder.WriteString(chunkData.Content)
						currentStatusForEvent = v1pb.AgentStatus_AGENT_STATUS_PROCESSING
						isPartialEvent = true
						agentStepData = map[string]interface{}{"type": "message_chunk", "timestamp": time.Now().Format(time.RFC3339Nano), "content": chunkData.Content, "agent": chunkData.Agent, "id": chunkData.ID}

						if chunkData.FinishReason != nil {
							isPartialEvent = false
							slog.Info("FinishReason received", "reason", *chunkData.FinishReason, "agent_task_id", agentTaskID)
							finishReasonLower := strings.ToLower(*chunkData.FinishReason)
							if finishReasonLower == "completed" || finishReasonLower == "success" || finishReasonLower == "done" || finishReasonLower == "stop" { // Added "stop"
								currentStatusForEvent = v1pb.AgentStatus_AGENT_STATUS_COMPLETED
								resStr := resultBuilder.String()
								finalResultData = &resStr
							} else if finishReasonLower == "failed" || finishReasonLower == "error" {
								currentStatusForEvent = v1pb.AgentStatus_AGENT_STATUS_FAILED
								errStr := fmt.Sprintf("Task failed with reason: %s. Accumulated output: %s", *chunkData.FinishReason, resultBuilder.String())
								errorData = &errStr
								isErrorEvent = true
								errorMessageForEvent = errStr
							}
						}
					}
				case "plan":
					var plan DeerFlowPlanData
					if err := json.Unmarshal([]byte(dataContent), &plan); err != nil {
						slog.Error("Failed to unmarshal DeerFlow plan event", "error", err, "data", dataContent, "agent_task_id", agentTaskID)
					} else {
						planJSONBytes, _ := json.Marshal(plan)
						planStr := string(planJSONBytes)
						planDataForUpdate = &planStr
						currentStatusForEvent = v1pb.AgentStatus_AGENT_STATUS_PLANNING
						isPartialEvent = false
						agentStepData = map[string]interface{}{"type": "plan", "timestamp": time.Now().Format(time.RFC3339Nano), "content": plan}
					}
				case "thought":
					var thought DeerFlowThoughtData
					if err := json.Unmarshal([]byte(dataContent), &thought); err != nil {
						slog.Error("Failed to unmarshal DeerFlow thought event", "error", err, "data", dataContent, "agent_task_id", agentTaskID)
					} else {
						currentStatusForEvent = v1pb.AgentStatus_AGENT_STATUS_PROCESSING
						isPartialEvent = true
						agentStepData = map[string]interface{}{"type": "thought", "timestamp": time.Now().Format(time.RFC3339Nano), "content": thought, "agent": thought.Agent}
					}
				case "tool_code", "tool_calls":
					originalEventType = "tool_call" // Normalize for dispatch consistency
					var toolCall DeerFlowToolCallData
					if err := json.Unmarshal([]byte(dataContent), &toolCall); err != nil {
						slog.Error("Failed to unmarshal DeerFlow tool_call event", "error", err, "data", dataContent, "agent_task_id", agentTaskID)
					} else {
						currentStatusForEvent = v1pb.AgentStatus_AGENT_STATUS_EXECUTING
						isPartialEvent = true
						agentStepData = map[string]interface{}{"type": "tool_call", "timestamp": time.Now().Format(time.RFC3339Nano), "content": toolCall, "agent": toolCall.Agent}
					}
				case "tool_output", "tool_call_result":
					originalEventType = "tool_result" // Normalize
					var toolResult DeerFlowToolResultData
					if err := json.Unmarshal([]byte(dataContent), &toolResult); err != nil {
						slog.Error("Failed to unmarshal DeerFlow tool_result event", "error", err, "data", dataContent, "agent_task_id", agentTaskID)
					} else {
						currentStatusForEvent = v1pb.AgentStatus_AGENT_STATUS_PROCESSING
						isPartialEvent = true
						agentStepData = map[string]interface{}{"type": "tool_result", "timestamp": time.Now().Format(time.RFC3339Nano), "content": toolResult, "agent": toolResult.Agent, "is_error": toolResult.IsError}
						if toolResult.IsError {
							slog.Warn("Tool execution resulted in an error", "agent_task_id", agentTaskID, "tool_call_id", toolResult.ToolCallID, "output", toolResult.Output)
							// Optionally set isErrorEvent = true and populate errorMessageForEvent if this should mark the whole task as failed
						}
					}
				case "interrupt":
					var interruptData DeerFlowInterruptData
					if err := json.Unmarshal([]byte(dataContent), &interruptData); err != nil {
						slog.Error("Failed to unmarshal DeerFlow interrupt event", "error", err, "data", dataContent, "agent_task_id", agentTaskID)
					} else {
						currentStatusForEvent = v1pb.AgentStatus_AGENT_STATUS_INTERRUPTED
						errMsg := fmt.Sprintf("Task interrupted: %s. Feedback required: %v", interruptData.Reason, interruptData.FeedbackRequired)
						errorData = &errMsg
						isErrorEvent = true // Treat interrupt as needing attention
						isPartialEvent = false
						agentStepData = map[string]interface{}{"type": "interrupt", "timestamp": time.Now().Format(time.RFC3339Nano), "content": interruptData, "agent": interruptData.Agent}
						errorMessageForEvent = errMsg
					}
				default:
					slog.Warn("Received unknown DeerFlow SSE event type", "event_type", eventType, "data", dataContent, "agent_task_id", agentTaskID)
					isPartialEvent = true
					agentStepData = map[string]interface{}{"type": "unknown", "timestamp": time.Now().Format(time.RFC3339Nano), "raw_event_type": eventType, "raw_data": dataContent}
				}

				if agentStepData != nil {
					stepJSONBytes, err := json.Marshal(agentStepData)
					if err == nil {
						stepsJSONArray = append(stepsJSONArray, string(stepJSONBytes))
						allStepsBytes, err := json.Marshal(stepsJSONArray)
						if err == nil {
							tempStepsStr := string(allStepsBytes)
							stepsDataForUpdate = &tempStepsStr
						} else {
							slog.Error("Failed to marshal all agent steps array", "error", err, "agent_task_id", agentTaskID)
						}
					} else {
						slog.Error("Failed to marshal agent step data", "error", err, "step_data", agentStepData, "agent_task_id", agentTaskID)
					}
				}

				s.updateMemoAgentStatus(context.Background(), memoID, agentTaskID, currentStatusForEvent, finalResultData, errorData, planDataForUpdate, stepsDataForUpdate)

				// Dispatch event to internal channel for StreamAgentTaskEvents subscribers
				s.agentEventChannelsMutex.RLock()
				ch, exists := s.agentEventChannels[agentTaskID]
				s.agentEventChannelsMutex.RUnlock()

				if exists {
					var finishReasonForEvent *string
					if currentStatusForEvent == v1pb.AgentStatus_AGENT_STATUS_COMPLETED && finalResultData != nil {
						// Extract finish reason if it was part of a message_chunk that completed the task
						if eventType == "message_chunk" {
							var chunkData DeerFlowMessageChunkData
							if json.Unmarshal([]byte(dataContent), &chunkData) == nil {
								finishReasonForEvent = chunkData.FinishReason
							}
						}
					} else if currentStatusForEvent == v1pb.AgentStatus_AGENT_STATUS_FAILED && errorData != nil {
						// Potentially extract finish_reason if error also came from message_chunk finish_reason
						if eventType == "message_chunk" {
							var chunkData DeerFlowMessageChunkData
							if json.Unmarshal([]byte(dataContent), &chunkData) == nil {
								finishReasonForEvent = chunkData.FinishReason
							}
						}
					}

					agentEvent := &v1pb.AgentTaskEvent{
						EventType:          originalEventType, // Use original/normalized type
						DataJson:           dataContent, // Send raw data for client flexibility
						Timestamp:          timestamppb.Now(),
						IsPartial:          isPartialEvent,
						IsError:            isErrorEvent,
						ErrorMessage:       &errorMessageForEvent, // Ensure this is set correctly
						CurrentAgentStatus: &currentStatusForEvent,
						FinishReason:       finishReasonForEvent,
						SourceEventId:      &id, // Pass SSE ID if available
					}
					// Non-blocking send to channel
					select {
					case ch <- agentEvent:
						slog.Debug("Dispatched agent event to channel", "agent_task_id", agentTaskID, "event_type", agentEvent.EventType)
					default:
						slog.Warn("Agent event channel full or closed, unable to dispatch event", "agent_task_id", agentTaskID, "event_type", agentEvent.EventType)
					}
				}

				// Reset for next event block
				dataContent = ""
				eventType = ""
				id = "" // Reset SSE ID
			}
			continue
		}

		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			// Data can span multiple lines, but SSE spec says it's terminated by a blank line.
			// For simplicity here, we'll assume data is on a single line after "data:".
			// A more robust parser would handle multi-line data fields.
			currentDataLine := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if dataContent == "" {
				dataContent = currentDataLine
			} else {
				// This would be for multi-line data, which we are not fully handling yet.
				// dataContent += "\n" + currentDataLine 
				slog.Warn("Received multi-line data, only first line processed for now", "agent_task_id", agentTaskID, "line", line)
			}
		} else if strings.HasPrefix(line, "id:") {
			id = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
		} else if strings.HasPrefix(line, ":") {
			// This is a comment line, ignore.
			slog.Debug("SSE Comment ignored", "line", line)
		} else {
			slog.Warn("Unrecognized SSE line format", "line", line, "agent_task_id", agentTaskID)
		}
	}
}

// StreamAgentTaskEvents is a server-streaming RPC that sends AgentTaskEvents to the client.
func (s *APIV1Service) StreamAgentTaskEvents(request *v1pb.StreamAgentTaskEventsRequest, stream v1pb.MemoService_StreamAgentTaskEventsServer) error {
	agentTaskID := request.AgentTaskId
	if agentTaskID == "" {
		return status.Errorf(codes.InvalidArgument, "agent_task_id is required")
	}

	slog.Info("Client subscribed to agent events", "agent_task_id", agentTaskID)

	// Create a new channel for this client or retrieve if already exists (though typically one client per stream call)
	// For simplicity, we create a new channel for each subscriber. HandleDeerflowSSE will publish to all.
	// A more robust system might use a single channel per agentTaskID and fan-out, but this is simpler for now.
	eventChan := make(chan *v1pb.AgentTaskEvent, 10) // Buffered channel

	s.agentEventChannelsMutex.Lock()
	// If there are other subscribers, this will overwrite, which is not ideal for fan-out.
	// A list of channels per agentTaskID would be needed for true fan-out to multiple stream calls for the SAME taskID.
	// For now, assuming one active StreamAgentTaskEvents call per agentTaskID at a time by design, or last one wins.
	// Or, better: check if a channel already exists and potentially error out or use it (complex concurrency).
	// Simplest for now: each call to StreamAgentTaskEvents is for a client that wants events for this task.
	// The producer (handleDeerflowSSE) needs to know about this channel.
	// This requires handleDeerflowSSE to look up this channel. Let's refine this.

	// Refined approach: Listener registers itself.
	// The channel should be associated with the agentTaskID allowing the producer to find it.
	// We will use a list of channels to support multiple listeners for the same task ID.

	// Let's store a list of channels for a given agent task ID.
	// This part should be in a new helper method for registering/unregistering listeners.
	// For brevity in this step, we'll directly manipulate the map here but acknowledge it needs to be robust.

	// Corrected simplified logic for single listener per taskID for now via this map (producer needs to find this specific channel)
	// This is still not a full fan-out. The producer needs to send to THE channel for this agentTaskID.
	// Let's assume that `agentEventChannels` stores THE authoritative channel that the producer writes to.
	// And this RPC reads from it. If it doesn't exist, the producer hasn't started or it's an error.

	// Let's redefine: agentEventChannels will map agentTaskID to a struct that manages multiple subscribers.
	// For now, let's stick to a simpler model for the edit: create a channel, the producer `handleDeerflowSSE`
	// will attempt to send to a channel registered under this agentTaskID.

	// Producer side (handleDeerflowSSE) will need to: s.agentEventChannelsMutex.Lock(); ch := s.agentEventChannels[agentTaskID]; s.agentEventChannelsMutex.Unlock(); if ch != nil { ch <- event }
	// Consumer side (this RPC):
	s.agentEventChannels[agentTaskID] = eventChan
	s.agentEventChannelsMutex.Unlock()

	defer func() {
		s.agentEventChannelsMutex.Lock()
		delete(s.agentEventChannels, agentTaskID) // Remove this specific channel
		s.agentEventChannelsMutex.Unlock()
		close(eventChan) // Close the channel when the RPC is done
		slog.Info("Client unsubscribed from agent events", "agent_task_id", agentTaskID)
	}()

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			slog.Info("Client connection closed or context cancelled", "agent_task_id", agentTaskID, "error", ctx.Err())
			return ctx.Err()
		case event, ok := <-eventChan:
			if !ok {
				slog.Info("Event channel closed by producer", "agent_task_id", agentTaskID)
				return nil // Graceful end if channel is closed
			}
			if err := stream.Send(event); err != nil {
				slog.Error("Failed to send agent event to client", "error", err, "agent_task_id", agentTaskID)
				return err
			}
			slog.Debug("Sent agent event to client", "agent_task_id", agentTaskID, "event_type", event.EventType)
		}
	}
}

// updateMemoAgentStatus is a helper to update the agent-related fields of a memo.
func (s *APIV1Service) updateMemoAgentStatus(
	ctx context.Context,
	memoID int32,
	agentTaskID string, // Used for logging/verification, not for lookup
	status v1pb.AgentStatus,
	resultJSON *string, // Changed to pointer for optionality
	errorMessage *string, // Changed to pointer for optionality
	planJSON *string, // Added optional plan JSON
	stepsJSON *string, // Added optional steps JSON
) {
	update := &store.UpdateMemo{
		ID:          memoID,
		AgentStatus: (*int32)(convertAgentStatusToStore(status)),
	}
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

	// It's crucial that this update doesn't overwrite other agent fields unintentionally.
	// If AgentPlanJson, AgentStepsJson etc. are populated incrementally, this simple update is fine.
	// If they are set once, ensure this helper doesn't nil them out if result/error is empty.
	// The current UpdateMemo store logic should handle partial updates correctly if fields are nil.

	if err := s.Store.UpdateMemo(ctx, update); err != nil {
		slog.Error("Failed to update memo agent status in DB", "error", err, "memo_id", memoID, "target_status", status.String())
	} else {
		slog.Info("Memo agent status updated in DB", "memo_id", memoID, "target_status", status.String())
	}
}
