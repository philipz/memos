import React, { useState, useEffect } from "react";
// 假設的 gRPC 客戶端實例和生成的類型路徑，需要根據實際項目結構調整
import { memoServiceClient } from "../grpcweb";
// import { ExecuteAgentOnMemoRequest, ExecuteAgentOnMemoRequest_MCPSettingsOverride } from "../../proto/memo_service_pb";
import { type Memo, type ExecuteAgentOnMemoRequest, AgentStatus } from "../types/proto/api/v1/memo_service";

// 輔助函數：從 memo.name (e.g., "memos/123") 中提取數字 ID
const getMemoIdFromName = (name: string): number => {
  const parts = name.split("/");
  return parseInt(parts[parts.length - 1] || "0", 10);
};

// 臨時的 mock 服務和類型，直到 gRPC 客戶端和 PB 文件正確集成
const mockMemoServiceClient = {
  executeAgentOnMemo: (request: ExecuteAgentOnMemoRequest /*metadata: any*/) => {
    // metadata removed
    console.log("Mock executeAgentOnMemo called with request:", request);
    const memoName = request.name;
    const numericMemoId = getMemoIdFromName(memoName);

    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (numericMemoId % 2 === 0) {
          console.log("Mock executeAgentOnMemo success for memoName:", memoName);
          // 模擬成功時返回 Agent ID 和初始狀態
          resolve({ agentTaskId: `agent-task-${Date.now()}`, initialStatus: AgentStatus.PENDING });
        } else {
          console.error("Mock executeAgentOnMemo failed for memoName:", memoName);
          reject(new Error("Mock API call failed"));
        }
      }, 500);
    });
  },
  // streamAgentTaskEvents 的 mock 應在 AgentWorkspace 中或專門的 service 中處理
};

interface AgentConfigModalProps {
  memo: Memo; // 使用從 protobuf 導入的 Memo 類型
  onClose: () => void;
  onStartAgent: (agentTaskId: string, initialQueryText: string, initialStatus: AgentStatus) => void; // 更新回調參數
}

const AgentConfigModal: React.FC<AgentConfigModalProps> = ({ memo, onClose, onStartAgent }) => {
  const [queryText, setQueryText] = useState(memo.content || "");
  const [autoAcceptPlan, setAutoAcceptPlan] = useState(true);
  const [maxPlanIterations, setMaxPlanIterations] = useState<number | string>(5);
  const [maxStepNum, setMaxStepNum] = useState<number | string>(10);
  const [maxSearchResults, setMaxSearchResults] = useState<number | string>(5);
  const [enableBackgroundInvestigation, setEnableBackgroundInvestigation] = useState(true);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const numericMemoId = getMemoIdFromName(memo.name);

  useEffect(() => {
    setQueryText(memo.content || "");
  }, [memo.content]);

  const handleSave = async () => {
    setIsLoading(true);
    setError(null);

    const requestParams: ExecuteAgentOnMemoRequest = {
      name: memo.name, // 使用 memo.name
      queryText: queryText || undefined,
      autoAcceptedPlan: autoAcceptPlan,
      maxPlanIterations: typeof maxPlanIterations === "string" ? parseInt(maxPlanIterations, 10) : maxPlanIterations,
      maxStepNum: typeof maxStepNum === "string" ? parseInt(maxStepNum, 10) : maxStepNum,
      maxSearchResults: typeof maxSearchResults === "string" ? parseInt(maxSearchResults, 10) : maxSearchResults,
      enableBackgroundInvestigation: enableBackgroundInvestigation,
      mcpSettingsOverride: {},
      // interruptFeedback 字段在初次調用時通常不設置
    };

    try {
      const response = await memoServiceClient.executeAgentOnMemo(requestParams, {}); // 真實API調用
      // const response = await mockMemoServiceClient.executeAgentOnMemo(requestParams /*, {}*/); // Mock API 調用, {} removed

      // 假設 response 包含 agentTaskId 和 initialStatus
      onStartAgent((response as any).agentTaskId, queryText, (response as any).initialStatus);
      onClose();
    } catch (err: any) {
      console.error("Failed to start agent task:", err);
      setError(err.message || "An unexpected error occurred");
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4 overflow-auto">
      <div className="bg-white dark:bg-zinc-800 p-6 rounded-lg shadow-xl w-full max-w-lg max-h-full overflow-y-auto">
        <h2 className="text-xl font-semibold mb-4 text-gray-800 dark:text-gray-200">Configure Agent for Memo: {numericMemoId}</h2>

        <div className="space-y-4">
          <div>
            <label htmlFor="queryText" className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Query Text (defaults to memo content)
            </label>
            <textarea
              id="queryText"
              value={queryText}
              onChange={(e) => setQueryText(e.target.value)}
              rows={4}
              className="w-full p-2 border border-gray-300 dark:border-zinc-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 dark:bg-zinc-700 dark:text-gray-200"
            />
          </div>

          <div className="flex items-center">
            <input
              id="autoAcceptPlan"
              type="checkbox"
              checked={autoAcceptPlan}
              onChange={(e) => setAutoAcceptPlan(e.target.checked)}
              className="h-4 w-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500 dark:bg-zinc-700 dark:border-zinc-600"
            />
            <label htmlFor="autoAcceptPlan" className="ml-2 block text-sm text-gray-900 dark:text-gray-300">
              Automatically accept initial plan
            </label>
          </div>

          <div>
            <label htmlFor="maxPlanIterations" className="block text-sm font-medium text-gray-700 dark:text-gray-300">
              Max Plan Iterations
            </label>
            <input
              type="number"
              id="maxPlanIterations"
              value={maxPlanIterations}
              onChange={(e) => setMaxPlanIterations(e.target.value === "" ? "" : parseInt(e.target.value, 10))}
              className="mt-1 w-full p-2 border border-gray-300 dark:border-zinc-600 rounded-md shadow-sm dark:bg-zinc-700 dark:text-gray-200"
            />
          </div>

          <div>
            <label htmlFor="maxStepNum" className="block text-sm font-medium text-gray-700 dark:text-gray-300">
              Max Execution Steps
            </label>
            <input
              type="number"
              id="maxStepNum"
              value={maxStepNum}
              onChange={(e) => setMaxStepNum(e.target.value === "" ? "" : parseInt(e.target.value, 10))}
              className="mt-1 w-full p-2 border border-gray-300 dark:border-zinc-600 rounded-md shadow-sm dark:bg-zinc-700 dark:text-gray-200"
            />
          </div>

          <div>
            <label htmlFor="maxSearchResults" className="block text-sm font-medium text-gray-700 dark:text-gray-300">
              Max Search Results (per step)
            </label>
            <input
              type="number"
              id="maxSearchResults"
              value={maxSearchResults}
              onChange={(e) => setMaxSearchResults(e.target.value === "" ? "" : parseInt(e.target.value, 10))}
              className="mt-1 w-full p-2 border border-gray-300 dark:border-zinc-600 rounded-md shadow-sm dark:bg-zinc-700 dark:text-gray-200"
            />
          </div>

          <div className="flex items-center">
            <input
              id="enableBackgroundInvestigation"
              type="checkbox"
              checked={enableBackgroundInvestigation}
              onChange={(e) => setEnableBackgroundInvestigation(e.target.checked)}
              className="h-4 w-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500 dark:bg-zinc-700 dark:border-zinc-600"
            />
            <label htmlFor="enableBackgroundInvestigation" className="ml-2 block text-sm text-gray-900 dark:text-gray-300">
              Enable Background Investigation (if supported by agent)
            </label>
          </div>

          {error && <div className="text-red-500 text-sm">Error: {error}</div>}
        </div>

        <div className="mt-6 flex justify-end space-x-3">
          <button
            type="button"
            onClick={onClose}
            disabled={isLoading}
            className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md shadow-sm hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 dark:bg-zinc-700 dark:text-gray-300 dark:border-zinc-600 dark:hover:bg-zinc-600"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={handleSave}
            disabled={isLoading}
            className="px-4 py-2 text-sm font-medium text-white bg-blue-600 border border-transparent rounded-md shadow-sm hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50"
          >
            {isLoading ? "Starting..." : "Start Agent"}
          </button>
        </div>
      </div>
    </div>
  );
};

export default AgentConfigModal;
