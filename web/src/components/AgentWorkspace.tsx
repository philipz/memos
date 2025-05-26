import React, { useState, useEffect, useRef } from "react";
// 假設的 gRPC 客戶端和生成的類型路徑
import { memoServiceClient } from "../grpcweb";
// import { StreamAgentTaskEventsRequest, AgentTaskEvent } from "../../proto/memo_service_pb";
import { AgentStatus, type AgentTaskEvent, type StreamAgentTaskEventsRequest, ExecuteAgentOnMemoRequest } from "../types/proto/api/v1/memo_service";

// 模擬 AgentTaskEvent 的類型 (與 useAgentStore.ts 中 DisplayEvent 類似，但更原始)
interface MockAgentTaskEventFromStream {
  memoId: number; // Not part of actual AgentTaskEvent, context for mock
  eventId: string; // Corresponds to sourceEventId in AgentTaskEvent
  eventType: string; // e.g., 'PLAN_GENERATED', 'STEP_EXECUTED', 'USER_INTERRUPT_REQUIRED', 'TASK_COMPLETED', 'TASK_FAILED', 'MESSAGE_CHUNK', 'TOOL_CALL_REQUESTED', 'TOOL_CALL_COMPLETED'
  dataJson: string; // JSON string of event-specific data
  timestamp: number; // Unix timestamp (seconds), to be converted to Date for AgentTaskEvent
}

// 用於提交反饋的 Mock 類型
interface MockFeedbackRequest {
  agentTaskId: string;
  feedback: string;
  // memoId is contextually known in the component
}

// 模擬 gRPC Stream - MockEventStream is unused, so commenting out or removing if not needed elsewhere
// class MockEventStream { ... }

// 模擬 gRPC 服務客戶端
const mockGrpcClient = {
  streamAgentTaskEvents: (request: { getMemoId: () => number; getAgentTaskId: () => string } /*, metadata: any*/) => {
    console.log("Mock streamAgentTaskEvents called for memoId:", request.getMemoId(), "agentTaskId:", request.getAgentTaskId());
    const listeners: { [key: string]: (eventOrError: any) => void } = {}; // More specific type for callback
    let eventCount = 0;
    const maxEvents = 10;
    let intervalId: number | null = null;

    const samplePlan = ["Step 1: Do A", "Step 2: Do B"];

    const emitEvent = () => {
      if (eventCount >= maxEvents) {
        if (intervalId) clearInterval(intervalId);
        if (listeners.end) listeners.end(null); // Pass null or a specific end event if needed
        return;
      }

      let eventType = "MESSAGE_CHUNK";
      let data: any = { message: `Event ${eventCount + 1} content...` };
      let currentAgentStatus = AgentStatus.EXECUTING;
      const isErrorForThisMockEvent = false;
      const errorMessageForThisMockEvent: string | undefined = undefined;

      if (eventCount === 0) {
        eventType = "PLAN_GENERATED";
        data = { plan: samplePlan };
        currentAgentStatus = AgentStatus.PLANNING;
      }
      if (eventCount === 3) {
        eventType = "USER_INTERRUPT_REQUIRED";
        data = {
          message: "Plan requires approval.",
          plan: ["Step 1: Do A (modified)", "Step 2: Do B"],
          options: [
            { text: "Approve Plan", value: "approved" },
            { text: "Reject", value: "rejected" },
          ],
        };
        currentAgentStatus = AgentStatus.AGENT_STATUS_INTERRUPTED;
      }
      if (eventCount === 5) {
        eventType = "TOOL_CALL_REQUESTED";
        data = { tool_name: "search_web", tool_input: { query: "latest AI news" } };
      }
      if (eventCount === 6) {
        eventType = "TOOL_CALL_COMPLETED";
        data = { tool_name: "search_web", tool_output: { summary: "AI is advancing rapidly." } };
      }
      if (eventCount === maxEvents - 1) {
        eventType = "TASK_COMPLETED";
        data = { result: "All tasks finished successfully!" };
        currentAgentStatus = AgentStatus.COMPLETED;
      }

      const mockEvent: MockAgentTaskEventFromStream = {
        memoId: request.getMemoId(),
        eventId: `event-${Date.now()}-${eventCount}`,
        eventType: eventType,
        dataJson: JSON.stringify(data),
        timestamp: Math.floor(Date.now() / 1000),
      };

      const processedEventForListener: AgentTaskEvent = {
        eventType: mockEvent.eventType,
        dataJson: mockEvent.dataJson,
        timestamp: new Date(mockEvent.timestamp * 1000),
        isPartial: mockEvent.eventType === "MESSAGE_CHUNK",
        isError: isErrorForThisMockEvent,
        errorMessage: errorMessageForThisMockEvent,
        currentAgentStatus: currentAgentStatus,
        sourceEventId: mockEvent.eventId,
      };

      if (listeners.data) listeners.data(processedEventForListener);
      eventCount++;
    };

    intervalId = window.setInterval(emitEvent, 1500);

    return {
      on: (type: string, callback: (eventOrError: any) => void) => {
        (listeners as any)[type] = callback;
        return this;
      },
      cancel: () => {
        console.log("Mock stream cancelled for memoId:", request.getMemoId());
        if (intervalId) clearInterval(intervalId);
        if (listeners.error) listeners.error(new Error("Stream cancelled by client"));
      },
    };
  },
  submitInterruptFeedback: async (request: MockFeedbackRequest): Promise<object> => {
    console.log("Mock submitInterruptFeedback called with:", request);
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve({});
      }, 300);
    });
  },
};

interface AgentWorkspaceProps {
  agentTaskId: string;
  initialQueryText: string;
  initialStatus: AgentStatus;
  memoId: number;
  onClose: () => void;
}

const AgentWorkspace: React.FC<AgentWorkspaceProps> = ({ agentTaskId, initialQueryText, initialStatus, memoId, onClose }) => {
  const [currentStatus, setCurrentStatus] = useState<AgentStatus>(initialStatus);
  const [events, setEvents] = useState<AgentTaskEvent[]>([]);
  // const [currentPlan, setCurrentPlan] = useState<string[] | null>(null); // Unused
  const [interruptUIData, setInterruptUIData] = useState<any | null>(null);
  const [userFeedback, setUserFeedback] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const streamRef = useRef<any>(null);

  useEffect(() => {
    setError(null);
    // const mockRequest = { getMemoId: () => memoId, getAgentTaskId: () => agentTaskId }; // 註釋掉 mockRequest

    // streamRef.current = mockGrpcClient.streamAgentTaskEvents(mockRequest); // 註釋掉 mock 調用
    const streamRequest: StreamAgentTaskEventsRequest = { agentTaskId: agentTaskId }; // 定義請求參數
    streamRef.current = memoServiceClient.streamAgentTaskEvents(streamRequest); // 使用真實 API 調用

    streamRef.current.on("data", (event: AgentTaskEvent) => {
      console.log("Received agent event:", event);
      setEvents((prev) => [...prev, event]);

      if (event.currentAgentStatus) {
        setCurrentStatus(event.currentAgentStatus);
      }
      if (event.isError && event.errorMessage) {
        setError(event.errorMessage);
      }

      const eventData = JSON.parse(event.dataJson);

      switch (event.eventType) {
        case "PLAN_GENERATED":
          // setCurrentPlan(eventData.plan); // currentPlan is unused
          break;
        case "USER_INTERRUPT_REQUIRED":
          setInterruptUIData(eventData);
          setCurrentStatus(AgentStatus.AGENT_STATUS_INTERRUPTED);
          break;
      }
    });

    streamRef.current.on("error", (err: Error) => {
      console.error("Stream error:", err);
      setError(err.message || "事件流連接失敗");
      setCurrentStatus(AgentStatus.FAILED);
      const errorEvent: AgentTaskEvent = {
        eventType: "ERROR_STREAM",
        dataJson: JSON.stringify({ error: err.message || "事件流連接失敗" }),
        timestamp: new Date(),
        isPartial: false,
        isError: true,
        errorMessage: err.message || "事件流連接失敗",
        currentAgentStatus: AgentStatus.FAILED,
        sourceEventId: `stream-error-${Date.now()}`,
      };
      setEvents((prev) => [...prev, errorEvent]);
    });

    streamRef.current.on("end", () => {
      console.log("Stream ended for memoId:", memoId);
      if (currentStatus !== AgentStatus.COMPLETED && currentStatus !== AgentStatus.FAILED && currentStatus !== AgentStatus.CANCELED) {
        setCurrentStatus(AgentStatus.COMPLETED);
      }
    });

    return () => {
      if (streamRef.current) {
        streamRef.current.cancel();
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [agentTaskId, memoId, currentStatus]);

  const handleInterruptSubmit = async () => {
    if (!interruptUIData || !userFeedback) return;
    setIsLoading(true);
    setError(null);

    const requestParams: ExecuteAgentOnMemoRequest = {
      name: `memos/${memoId}`, // Construct memo name from memoId
      interruptFeedback: userFeedback,
      // agentTaskId is not directly part of ExecuteAgentOnMemoRequest for feedback,
      // it's implicitly handled by the backend via memo_name and active task.
      // Other fields like queryText, etc., are not needed for feedback submission.
      mcpSettingsOverride: {}, // Needs to be present as it's not optional in the type
    };

    try {
      // await mockGrpcClient.submitInterruptFeedback(feedbackRequest); // 註釋掉 mock 調用
      await memoServiceClient.executeAgentOnMemo(requestParams); // 使用 executeAgentOnMemo 提交反饋
      setInterruptUIData(null);
      setUserFeedback("");
      // Optionally, update currentStatus or re-fetch/rely on stream for status update
      setCurrentStatus(AgentStatus.EXECUTING); // Assume feedback submission resumes execution
    } catch (err: any) {
      console.error("Failed to submit feedback:", err);
      setError(err.message || "Failed to submit feedback");
    } finally {
      setIsLoading(false);
    }
  };

  const renderEventContent = (event: AgentTaskEvent) => {
    let contentToDisplay: any;
    try {
      contentToDisplay = JSON.parse(event.dataJson);
    } catch (e) {
      contentToDisplay = { raw: event.dataJson, parseError: (e as Error).message };
    }

    switch (event.eventType) {
      case "MESSAGE_CHUNK":
        return <p style={{ margin: "0 0 5px 0" }}>{contentToDisplay?.message || event.dataJson}</p>;
      case "PLAN_GENERATED":
        return (
          <div>
            <strong>Plan:</strong>
            <ul>{contentToDisplay?.plan?.map((step: string, i: number) => <li key={i}>{step}</li>)}</ul>
          </div>
        );
      case "USER_INTERRUPT_REQUIRED":
        return (
          <p>
            <i>User action required. See details in interrupt section. Current plan: {contentToDisplay?.plan?.join(", ")}</i>
          </p>
        );
      case "TASK_COMPLETED":
        return (
          <p style={{ color: "green" }}>
            <strong>任務完成:</strong> {contentToDisplay?.result || event.dataJson}
          </p>
        );
      case "TASK_FAILED":
        return (
          <p style={{ color: "red" }}>
            <strong>任務失敗:</strong> {contentToDisplay?.error || event.errorMessage || event.dataJson}
          </p>
        );
      case "TOOL_CALL_REQUESTED":
        return (
          <p>
            <i>
              工具調用請求: {contentToDisplay?.tool_name} ({JSON.stringify(contentToDisplay?.tool_input)})
            </i>
          </p>
        );
      case "TOOL_CALL_COMPLETED":
        return (
          <p>
            <i>
              工具結果 ({contentToDisplay?.tool_name}): {JSON.stringify(contentToDisplay?.tool_output)}
            </i>
          </p>
        );
      case "ERROR_STREAM":
        return (
          <p style={{ color: "red" }}>
            <strong>事件流錯誤:</strong> {contentToDisplay?.error || event.dataJson}
          </p>
        );
      default:
        return <pre style={{ whiteSpace: "pre-wrap", wordBreak: "break-all" }}>{event.dataJson}</pre>;
    }
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-[60] p-4">
      <div className="bg-white dark:bg-zinc-800 p-6 rounded-lg shadow-xl w-full max-w-2xl max-h-[90vh] flex flex-col">
        <h2 className="text-xl font-semibold mb-4 text-gray-800 dark:text-gray-200">Agent Workspace (Task: {agentTaskId})</h2>
        <p className="text-sm text-gray-600 dark:text-gray-400 mb-1">Memo ID: {memoId}</p>
        <p className="text-sm text-gray-600 dark:text-gray-400 mb-1">Query: "{initialQueryText}"</p>
        <p className="text-sm font-medium mb-3" style={{ color: currentStatus === AgentStatus.FAILED ? "red" : "inherit" }}>
          Status: {Object.keys(AgentStatus)[Object.values(AgentStatus).indexOf(currentStatus)] || "N/A"}
        </p>

        {error && <p className="text-red-500 text-sm mb-3">Error: {error}</p>}

        {interruptUIData && currentStatus === AgentStatus.AGENT_STATUS_INTERRUPTED && (
          <div
            className="mb-4 p-3 border border-dashed border-blue-500 rounded-md bg-blue-50 dark:bg-blue-900 dark:border-blue-700"
            style={{ border: "1px dashed blue", padding: "10px", marginBottom: "10px" }}
          >
            <h3 className="font-semibold text-blue-700 dark:text-blue-300">User Action Required</h3>
            <p className="text-sm text-blue-600 dark:text-blue-400">{interruptUIData.message}</p>
            {interruptUIData.plan && (
              <ul className="list-disc list-inside mt-2 text-sm">
                {interruptUIData.plan.map((step: string, i: number) => (
                  <li key={i}>{step}</li>
                ))}
              </ul>
            )}
            <textarea
              value={userFeedback}
              onChange={(e) => setUserFeedback(e.target.value)}
              placeholder={
                interruptUIData.options
                  ? `Provide feedback or choose an option (e.g., '${interruptUIData.options[0]?.value}')`
                  : "Provide feedback"
              }
              rows={2}
              className="w-full p-2 mt-2 border border-blue-300 rounded-md dark:bg-blue-800 dark:text-gray-200 dark:border-blue-600"
            />
            {interruptUIData.options?.map((opt: { text: string; value: string }) => (
              <button
                key={opt.value}
                onClick={() => {
                  setUserFeedback(opt.value);
                  handleInterruptSubmit();
                }}
                disabled={isLoading}
                className="mt-2 mr-2 px-3 py-1.5 text-sm font-medium text-white bg-blue-500 rounded-md hover:bg-blue-600 disabled:opacity-50"
              >
                {opt.text}
              </button>
            ))}
            {!interruptUIData.options && (
              <button
                onClick={handleInterruptSubmit}
                disabled={isLoading || !userFeedback}
                className="mt-2 px-3 py-1.5 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700 disabled:opacity-50"
                style={{ padding: "10px", marginTop: "5px" }}
              >
                {isLoading ? "Submitting..." : "Submit Feedback"}
              </button>
            )}
          </div>
        )}

        <div
          className="flex-grow overflow-y-auto border border-gray-300 dark:border-zinc-700 rounded-md p-3 bg-gray-50 dark:bg-zinc-900"
          style={{ maxHeight: "300px", overflowY: "auto", borderTop: "1px solid #ddd", paddingTop: "10px" }}
        >
          <h3 className="text-md font-semibold mb-2 text-gray-700 dark:text-gray-300">Event Log:</h3>
          {events.length === 0 && <p className="text-sm text-gray-500 dark:text-gray-400">No events yet.</p>}
          {events.map((event, index) => (
            <div
              key={event.sourceEventId || `event-${index}`}
              className="mb-2 pb-2 border-b border-gray-200 dark:border-zinc-700 last:border-b-0"
              style={{ marginBottom: "8px", paddingBottom: "8px", borderBottom: "1px solid #f0f0f0" }}
            >
              <p className="text-xs text-gray-500 dark:text-gray-400" style={{ color: "#777" }}>
                {event.eventType} ({event.timestamp ? new Date(event.timestamp).toLocaleTimeString() : "no timestamp"})
                {event.isPartial && " (partial)"}
                {event.isError && " (ERROR)"}
              </p>
              {renderEventContent(event)}
            </div>
          ))}
        </div>

        <div className="mt-6 flex justify-end">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md shadow-sm hover:bg-gray-50 dark:bg-zinc-700 dark:text-gray-300 dark:border-zinc-600 dark:hover:bg-zinc-600"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
};

export default AgentWorkspace;
