import { create, StateCreator } from "zustand";
import { immer } from "zustand/middleware/immer";
import { AgentStatus, type ExecuteAgentOnMemoRequest, type AgentTaskEvent } from "../../types/proto/api/v1/memo_service";

// import { type Memo } from "../../types/proto/api/v1/memo_service"; // Memo is not directly used in store state, but for context

// --- Mocking gRPC client and Protobuf types (應由實際 gRPC-web 生成的代碼替換) ---
// 假設的 gRPC 客戶端實例和生成的類型路徑
// import { memoServiceClient } from "../../grpc";
// import { ExecuteAgentOnMemoRequest as ActualExecuteAgentOnMemoRequest, StreamAgentTaskEventsRequest, AgentTaskEvent as ActualAgentTaskEvent, InterruptFeedback } from "../../../proto/memo_service_pb"; // Hypothetical path

// 簡易 mock gRPC client
const mockMemoServiceClient = {
  executeAgentOnMemo: async (request: ExecuteAgentOnMemoRequest): Promise<{ agentTaskId: string; initialStatus: AgentStatus }> => {
    console.log("Mock store executeAgentOnMemo called with request:", request);
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve({ agentTaskId: `store-agent-task-${Date.now()}`, initialStatus: AgentStatus.PENDING });
      }, 500);
    });
  },
  // streamAgentTaskEvents: (request, metadata, callback) => { ... } // Stream handling is more complex, usually not directly in store like this
  // submitInterruptFeedback: async (request) => { ... }
};

// --- Store Definition ---

export interface AgentTaskStep {
  stepNumber: number;
  description: string;
  status: "pending" | "executing" | "completed" | "failed";
  result?: string;
  error?: string;
}

export interface AgentTask {
  agentTaskId: string | null;
  memoId: number; // Assuming numeric ID for simplicity in store, matches AgentConfigModal
  queryText: string;
  status: AgentStatus;
  plan: string[] | null;
  steps: AgentTaskStep[];
  currentToolCall?: { name: string; input: any };
  result: any | null;
  error: string | null;
  rawEvents: AgentTaskEvent[]; // Store raw events for detailed log or debugging
  isConfigModalOpen: boolean; // UI state for this specific memo's agent config
  userInterruptRequired: boolean;
  interruptData: any | null; // Data associated with the interrupt (e.g. plan, options)
}

export interface AgentStoreState {
  activeTasks: Record<number, AgentTask>; // Keyed by memoId
  globalError: string | null;

  // Actions
  openConfigModal: (memoId: number, initialQueryText: string) => void;
  closeConfigModal: (memoId: number) => void;
  startAgentTask: (memoId: number, request: ExecuteAgentOnMemoRequest) => Promise<void>;
  handleAgentEvent: (memoId: number, event: AgentTaskEvent) => void; // Simplified, real would be stream based
  submitInterruptFeedback: (memoId: number, feedback: string) => Promise<void>;
  clearTaskState: (memoId: number) => void;
  getTask: (memoId: number) => AgentTask | undefined;
}

const agentStoreCreator: StateCreator<AgentStoreState, [["zustand/immer", never]], [], AgentStoreState> = (set, get) => ({
  activeTasks: {},
  globalError: null,

  openConfigModal: (memoId, initialQueryText) => {
    set((state) => {
      if (!state.activeTasks[memoId]) {
        // Initialize task state if it doesn't exist
        state.activeTasks[memoId] = {
          agentTaskId: null,
          memoId: memoId,
          queryText: initialQueryText,
          status: AgentStatus.AGENT_STATUS_UNSPECIFIED, // Or PENDING if modal opening implies intent
          plan: null,
          steps: [],
          result: null,
          error: null,
          rawEvents: [],
          isConfigModalOpen: true,
          userInterruptRequired: false,
          interruptData: null,
        };
      } else {
        state.activeTasks[memoId].isConfigModalOpen = true;
        state.activeTasks[memoId].queryText = initialQueryText; // Update query text if modal is reopened
      }
    });
  },

  closeConfigModal: (memoId) => {
    set((state) => {
      if (state.activeTasks[memoId]) {
        state.activeTasks[memoId].isConfigModalOpen = false;
      }
    });
  },

  startAgentTask: async (memoId, request) => {
    set((state) => {
      // Ensure task entry exists, update query text from actual request
      const queryText = request.queryText || state.activeTasks[memoId]?.queryText || "";
      state.activeTasks[memoId] = {
        ...state.activeTasks[memoId], // Spread existing to keep other fields like isConfigModalOpen
        memoId: memoId,
        agentTaskId: null, // Will be set by API response
        queryText: queryText,
        status: AgentStatus.PENDING,
        plan: null,
        steps: [],
        result: null,
        error: null,
        rawEvents: [],
        userInterruptRequired: false,
        interruptData: null,
        isConfigModalOpen: false, // Close modal on start
      };
      state.globalError = null;
    });

    try {
      const response = await mockMemoServiceClient.executeAgentOnMemo(request);
      set((state) => {
        if (state.activeTasks[memoId]) {
          state.activeTasks[memoId].agentTaskId = response.agentTaskId;
          state.activeTasks[memoId].status = response.initialStatus; // Should be PENDING
        }
      });
      // Here, you would typically initiate the event stream listening for this agentTaskId
      // For example: startListeningToAgentEvents(response.agentTaskId, memoId);
    } catch (error: any) {
      console.error("Failed to start agent task in store:", error);
      set((state) => {
        if (state.activeTasks[memoId]) {
          state.activeTasks[memoId].status = AgentStatus.FAILED;
          state.activeTasks[memoId].error = error.message || "Failed to start agent task";
        }
        state.globalError = error.message || "Failed to start agent task";
      });
    }
  },

  handleAgentEvent: (memoId, event) => {
    set((state) => {
      const task = state.activeTasks[memoId];
      if (!task) return;

      task.rawEvents.push(event);
      if (event.currentAgentStatus) {
        task.status = event.currentAgentStatus;
      }
      if (event.isError && event.errorMessage) {
        task.error = event.errorMessage;
        task.status = AgentStatus.FAILED; // Ensure status reflects error
      }

      const eventData = event.dataJson ? JSON.parse(event.dataJson) : {};

      switch (event.eventType) {
        case "PLAN_GENERATED":
          task.plan = eventData.plan;
          task.status = AgentStatus.PLANNING;
          break;
        case "USER_INTERRUPT_REQUIRED": // Mapped from actual AGENT_STATUS_INTERRUPTED if event types differ
          task.userInterruptRequired = true;
          task.interruptData = eventData;
          task.status = AgentStatus.AGENT_STATUS_INTERRUPTED;
          break;
        case "TASK_STEP_COMPLETED": // Assuming an event type for this
          {
            const stepIndex = task.steps.findIndex((s: AgentTaskStep) => s.stepNumber === eventData.stepNumber);
            if (stepIndex > -1) {
              task.steps[stepIndex].status = "completed";
              task.steps[stepIndex].result = eventData.result;
            } else {
              task.steps.push({
                stepNumber: eventData.stepNumber,
                description: eventData.description || "Unknown step",
                status: "completed",
                result: eventData.result,
              });
            }
          }
          break;
        case "TASK_STEP_FAILED": // Assuming an event type for this
          {
            const stepIndex = task.steps.findIndex((s: AgentTaskStep) => s.stepNumber === eventData.stepNumber);
            if (stepIndex > -1) {
              task.steps[stepIndex].status = "failed";
              task.steps[stepIndex].error = eventData.error;
            } else {
              task.steps.push({
                stepNumber: eventData.stepNumber,
                description: eventData.description || "Unknown step",
                status: "failed",
                error: eventData.error,
              });
            }
            task.status = AgentStatus.FAILED; // Overall task status
          }
          break;
        case "MESSAGE_CHUNK":
          // For simplicity, not accumulating chunks here, just noting the event
          // A real implementation might append to a 'currentMessage' field
          if (!task.status || task.status === AgentStatus.PLANNING || task.status === AgentStatus.PENDING) {
            task.status = AgentStatus.EXECUTING; // If receiving chunks, assume execution
          }
          break;
        case "TASK_COMPLETED":
          task.result = eventData.result;
          task.status = AgentStatus.COMPLETED;
          task.userInterruptRequired = false;
          task.interruptData = null;
          break;
        case "TASK_FAILED":
          task.error = eventData.error || task.error || "Unknown task failure";
          task.status = AgentStatus.FAILED;
          task.userInterruptRequired = false;
          task.interruptData = null;
          break;
        // Add other event type handlers as needed, e.g., for tool calls
      }
    });
  },

  submitInterruptFeedback: async (memoId, feedback) => {
    const task = get().activeTasks[memoId];
    if (!task || !task.agentTaskId) {
      console.error("Cannot submit feedback, no active task or agentTaskId for memoId:", memoId);
      set((state) => {
        state.globalError = "Cannot submit feedback: task not found.";
      });
      return;
    }

    set((state) => {
      if (state.activeTasks[memoId]) {
        state.activeTasks[memoId].userInterruptRequired = false;
        state.activeTasks[memoId].interruptData = null;
        // Optimistically set status, backend will confirm via new events
        state.activeTasks[memoId].status = AgentStatus.EXECUTING;
      }
      state.globalError = null;
    });

    try {
      // Actual API call: await memoServiceClient.submitInterruptFeedback({ agentTaskId: task.agentTaskId, feedback });
      console.log(`Mock submitInterruptFeedback for agentTaskId ${task.agentTaskId} with feedback: ${feedback}`);
      // After submitting, the backend should send new events. The store will react to these via handleAgentEvent.
    } catch (error: any) {
      console.error("Failed to submit interrupt feedback in store:", error);
      set((state) => {
        if (state.activeTasks[memoId]) {
          state.activeTasks[memoId].status = AgentStatus.AGENT_STATUS_INTERRUPTED; // Revert to interrupted on failure
          state.activeTasks[memoId].error = error.message || "Failed to submit feedback";
          state.activeTasks[memoId].userInterruptRequired = true; // Show UI again
        }
        state.globalError = error.message || "Failed to submit feedback";
      });
    }
  },

  clearTaskState: (memoId) => {
    set((state) => {
      delete state.activeTasks[memoId];
    });
  },

  getTask: (memoId) => {
    return get().activeTasks[memoId];
  },
});

export const useAgentStore = create<AgentStoreState>()(immer(agentStoreCreator));

export default useAgentStore;
