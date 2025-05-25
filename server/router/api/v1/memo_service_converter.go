package v1

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/usememos/gomark/parser"
	"github.com/usememos/gomark/parser/tokenizer"

	v1pb "github.com/usememos/memos/proto/gen/api/v1"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

func (s *APIV1Service) convertMemoFromStore(ctx context.Context, memo *store.Memo) (*v1pb.Memo, error) {
	displayTs := memo.CreatedTs
	workspaceMemoRelatedSetting, err := s.Store.GetWorkspaceMemoRelatedSetting(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get workspace memo related setting")
	}
	if workspaceMemoRelatedSetting.DisplayWithUpdateTime {
		displayTs = memo.UpdatedTs
	}

	name := fmt.Sprintf("%s%s", MemoNamePrefix, memo.UID)
	memoMessage := &v1pb.Memo{
		Name:        name,
		State:       convertStateFromStore(memo.RowStatus),
		Creator:     fmt.Sprintf("%s%d", UserNamePrefix, memo.CreatorID),
		CreateTime:  timestamppb.New(time.Unix(memo.CreatedTs, 0)),
		UpdateTime:  timestamppb.New(time.Unix(memo.UpdatedTs, 0)),
		DisplayTime: timestamppb.New(time.Unix(displayTs, 0)),
		Content:     memo.Content,
		Visibility:  convertVisibilityFromStore(memo.Visibility),
		Pinned:      memo.Pinned,
	}
	if memo.Payload != nil {
		memoMessage.Tags = memo.Payload.Tags
		memoMessage.Property = convertMemoPropertyFromStore(memo.Payload.Property)
		memoMessage.Location = convertLocationFromStore(memo.Payload.Location)
	}
	if memo.ParentID != nil {
		parent, err := s.Store.GetMemo(ctx, &store.FindMemo{
			ID:             memo.ParentID,
			ExcludeContent: true,
		})
		if err != nil {
			return nil, errors.Wrap(err, "failed to get parent memo")
		}
		parentName := fmt.Sprintf("%s%s", MemoNamePrefix, parent.UID)
		memoMessage.Parent = &parentName
	}

	listMemoRelationsResponse, err := s.ListMemoRelations(ctx, &v1pb.ListMemoRelationsRequest{Name: name})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list memo relations")
	}
	memoMessage.Relations = listMemoRelationsResponse.Relations

	listMemoResourcesResponse, err := s.ListMemoResources(ctx, &v1pb.ListMemoResourcesRequest{Name: name})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list memo resources")
	}
	memoMessage.Resources = listMemoResourcesResponse.Resources

	listMemoReactionsResponse, err := s.ListMemoReactions(ctx, &v1pb.ListMemoReactionsRequest{Name: name})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list memo reactions")
	}
	memoMessage.Reactions = listMemoReactionsResponse.Reactions

	nodes, err := parser.Parse(tokenizer.Tokenize(memo.Content))
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse content")
	}
	memoMessage.Nodes = convertFromASTNodes(nodes)

	snippet, err := getMemoContentSnippet(memo.Content)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get memo content snippet")
	}
	memoMessage.Snippet = snippet

	// Populate Agentic fields
	if memo.Payload != nil { // Assuming agent fields might be in payload or directly on memo store object
		// Adapt these lines based on where agent fields are actually stored in store.Memo
		// For now, let's assume they are directly on store.Memo and are nullable (e.g. *string, *int32)
		if memo.AgentTaskID != nil {
			memoMessage.AgentTaskId = *memo.AgentTaskID
		}
		if memo.AgentStatus != nil {
			memoMessage.AgentStatus = convertAgentStatusFromStore(*memo.AgentStatus)
		}
		if memo.AgentQueryText != nil {
			memoMessage.AgentQueryText = *memo.AgentQueryText
		}
		if memo.AgentPlanJson != nil {
			memoMessage.AgentPlanJson = *memo.AgentPlanJson
		}
		if memo.AgentStepsJson != nil {
			memoMessage.AgentStepsJson = *memo.AgentStepsJson
		}
		if memo.AgentResultJson != nil {
			memoMessage.AgentResultJson = *memo.AgentResultJson
		}
		if memo.AgentErrorMessage != nil {
			memoMessage.AgentErrorMessage = *memo.AgentErrorMessage
		}
	}

	return memoMessage, nil
}

func convertMemoPropertyFromStore(property *storepb.MemoPayload_Property) *v1pb.Memo_Property {
	if property == nil {
		return nil
	}
	return &v1pb.Memo_Property{
		HasLink:            property.HasLink,
		HasTaskList:        property.HasTaskList,
		HasCode:            property.HasCode,
		HasIncompleteTasks: property.HasIncompleteTasks,
	}
}

func convertLocationFromStore(location *storepb.MemoPayload_Location) *v1pb.Location {
	if location == nil {
		return nil
	}
	return &v1pb.Location{
		Placeholder: location.Placeholder,
		Latitude:    location.Latitude,
		Longitude:   location.Longitude,
	}
}

func convertLocationToStore(location *v1pb.Location) *storepb.MemoPayload_Location {
	if location == nil {
		return nil
	}
	return &storepb.MemoPayload_Location{
		Placeholder: location.Placeholder,
		Latitude:    location.Latitude,
		Longitude:   location.Longitude,
	}
}

func convertVisibilityFromStore(visibility store.Visibility) v1pb.Visibility {
	switch visibility {
	case store.Private:
		return v1pb.Visibility_PRIVATE
	case store.Protected:
		return v1pb.Visibility_PROTECTED
	case store.Public:
		return v1pb.Visibility_PUBLIC
	default:
		return v1pb.Visibility_VISIBILITY_UNSPECIFIED
	}
}

func convertVisibilityToStore(visibility v1pb.Visibility) store.Visibility {
	switch visibility {
	case v1pb.Visibility_PRIVATE:
		return store.Private
	case v1pb.Visibility_PROTECTED:
		return store.Protected
	case v1pb.Visibility_PUBLIC:
		return store.Public
	default:
		return store.Private
	}
}

func convertAgentStatusFromStore(status int32) v1pb.AgentStatus {
	switch status {
	case 1: // Corresponds to PENDING
		return v1pb.AgentStatus_PENDING
	case 2: // Corresponds to PLANNING
		return v1pb.AgentStatus_PLANNING
	case 3: // Corresponds to EXECUTING
		return v1pb.AgentStatus_EXECUTING
	case 4: // Corresponds to COMPLETED
		return v1pb.AgentStatus_COMPLETED
	case 5: // Corresponds to FAILED
		return v1pb.AgentStatus_FAILED
	case 6: // Corresponds to CANCELED
		return v1pb.AgentStatus_CANCELED
	default:
		return v1pb.AgentStatus_AGENT_STATUS_UNSPECIFIED
	}
}

func convertAgentStatusToStore(status v1pb.AgentStatus) int32 {
	switch status {
	case v1pb.AgentStatus_PENDING:
		return 1
	case v1pb.AgentStatus_PLANNING:
		return 2
	case v1pb.AgentStatus_EXECUTING:
		return 3
	case v1pb.AgentStatus_COMPLETED:
		return 4
	case v1pb.AgentStatus_FAILED:
		return 5
	case v1pb.AgentStatus_CANCELED:
		return 6
	default:
		return 0 // Corresponds to AGENT_STATUS_UNSPECIFIED
	}
}
