package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

// ElasticAPI 弹性伸缩API服务器
type ElasticAPI struct {
	controller *ElasticController
	router     *mux.Router
	server     *http.Server
}

// NewElasticAPI 创建弹性伸缩API服务器
func NewElasticAPI(controller *ElasticController, port string) *ElasticAPI {
	api := &ElasticAPI{
		controller: controller,
		router:     mux.NewRouter(),
	}

	// 设置路由
	api.setupRoutes()

	// 创建HTTP服务器
	api.server = &http.Server{
		Addr:         port,
		Handler:      api.router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	return api
}

// setupRoutes 设置API路由
func (api *ElasticAPI) setupRoutes() {
	// 基础路由
	api.router.HandleFunc("/health", api.healthCheck).Methods("GET")

	// 统计信息路由
	api.router.HandleFunc("/api/v1/elastic/stats", api.getStats).Methods("GET")
	api.router.HandleFunc("/api/v1/elastic/state-stats", api.getStateStats).Methods("GET")

	// 节点状态路由
	api.router.HandleFunc("/api/v1/elastic/nodes", api.getAllNodes).Methods("GET")
	api.router.HandleFunc("/api/v1/elastic/nodes/{nodeId}", api.getNode).Methods("GET")
	api.router.HandleFunc("/api/v1/elastic/nodes/{nodeId}/costs", api.updateNodeCosts).Methods("PUT")

	// 决策和历史路由
	api.router.HandleFunc("/api/v1/elastic/nodes/{nodeId}/decision", api.makeDecision).Methods("POST")
	api.router.HandleFunc("/api/v1/elastic/nodes/{nodeId}/simulate", api.simulateDecision).Methods("POST")
	api.router.HandleFunc("/api/v1/elastic/nodes/{nodeId}/history/decisions", api.getDecisionHistory).Methods("GET")
	api.router.HandleFunc("/api/v1/elastic/nodes/{nodeId}/history/transitions", api.getTransitionHistory).Methods("GET")

	// 强制操作路由
	api.router.HandleFunc("/api/v1/elastic/nodes/{nodeId}/force-scaling", api.forceScaling).Methods("POST")

	// 云实例路由
	api.router.HandleFunc("/api/v1/elastic/cloud/instances", api.getCloudInstances).Methods("GET")
	api.router.HandleFunc("/api/v1/elastic/cloud/providers", api.getCloudProviders).Methods("GET")
	api.router.HandleFunc("/api/v1/elastic/cloud/providers/{providerName}", api.addCloudProvider).Methods("POST")
	api.router.HandleFunc("/api/v1/elastic/cloud/providers/{providerName}", api.removeCloudProvider).Methods("DELETE")
}

// Start 启动API服务器
func (api *ElasticAPI) Start() error {
	return api.server.ListenAndServe()
}

// Stop 停止API服务器
func (api *ElasticAPI) Stop(ctx context.Context) error {
	return api.server.Shutdown(ctx)
}

// healthCheck 健康检查
func (api *ElasticAPI) healthCheck(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
		"service":   "elastic-controller",
	}
	api.writeJSONResponse(w, http.StatusOK, response)
}

// getStats 获取统计信息
func (api *ElasticAPI) getStats(w http.ResponseWriter, r *http.Request) {
	stats := api.controller.GetStats()
	api.writeJSONResponse(w, http.StatusOK, stats)
}

// getStateStats 获取状态统计
func (api *ElasticAPI) getStateStats(w http.ResponseWriter, r *http.Request) {
	stateStats := api.controller.GetStateStatistics()
	api.writeJSONResponse(w, http.StatusOK, stateStats)
}

// getAllNodes 获取所有节点
func (api *ElasticAPI) getAllNodes(w http.ResponseWriter, r *http.Request) {
	// 检查查询参数
	query := r.URL.Query()
	filter := query.Get("filter")

	var nodes map[string]*ElasticNodeInfo

	switch filter {
	case "active":
		nodes = api.controller.GetActiveNodes()
	case "scalable":
		nodes = api.controller.GetScalableNodes()
	default:
		nodes = api.controller.GetAllNodeStates()
	}

	api.writeJSONResponse(w, http.StatusOK, nodes)
}

// getNode 获取单个节点信息
func (api *ElasticAPI) getNode(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["nodeId"]

	nodeInfo, exists := api.controller.GetNodeState(nodeID)
	if !exists {
		api.writeErrorResponse(w, http.StatusNotFound, fmt.Sprintf("Node %s not found", nodeID))
		return
	}

	api.writeJSONResponse(w, http.StatusOK, nodeInfo)
}

// updateNodeCosts 更新节点成本
func (api *ElasticAPI) updateNodeCosts(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["nodeId"]

	var request struct {
		FixedCost    float64 `json:"fixed_cost"`
		VariableCost float64 `json:"variable_cost"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		api.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if err := api.controller.UpdateNodeCosts(nodeID, request.FixedCost, request.VariableCost); err != nil {
		api.writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	api.writeJSONResponse(w, http.StatusOK, map[string]string{"message": "Node costs updated successfully"})
}

// makeDecision 执行伸缩决策
func (api *ElasticAPI) makeDecision(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["nodeId"]

	var request struct {
		VirtualQueue float64 `json:"virtual_queue"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		api.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	decision, err := api.controller.ProcessScalingRequest(nodeID, request.VirtualQueue)
	if err != nil {
		api.writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	api.writeJSONResponse(w, http.StatusOK, decision)
}

// simulateDecision 模拟决策
func (api *ElasticAPI) simulateDecision(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["nodeId"]

	var request struct {
		VirtualQueue float64 `json:"virtual_queue"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		api.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	decision, err := api.controller.SimulateDecision(nodeID, request.VirtualQueue)
	if err != nil {
		api.writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	api.writeJSONResponse(w, http.StatusOK, decision)
}

// getDecisionHistory 获取决策历史
func (api *ElasticAPI) getDecisionHistory(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["nodeId"]

	history, exists := api.controller.GetDecisionHistory(nodeID)
	if !exists {
		api.writeErrorResponse(w, http.StatusNotFound, fmt.Sprintf("No decision history found for node %s", nodeID))
		return
	}

	// 支持分页
	query := r.URL.Query()
	limit := 50 // 默认限制
	if limitStr := query.Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			limit = l
		}
	}

	if len(history) > limit {
		history = history[len(history)-limit:]
	}

	api.writeJSONResponse(w, http.StatusOK, history)
}

// getTransitionHistory 获取状态转换历史
func (api *ElasticAPI) getTransitionHistory(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["nodeId"]

	history, exists := api.controller.GetTransitionHistory(nodeID)
	if !exists {
		api.writeErrorResponse(w, http.StatusNotFound, fmt.Sprintf("No transition history found for node %s", nodeID))
		return
	}

	// 支持分页
	query := r.URL.Query()
	limit := 50 // 默认限制
	if limitStr := query.Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			limit = l
		}
	}

	if len(history) > limit {
		history = history[len(history)-limit:]
	}

	api.writeJSONResponse(w, http.StatusOK, history)
}

// forceScaling 强制伸缩
func (api *ElasticAPI) forceScaling(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["nodeId"]

	var request struct {
		TargetState ElasticNodeState `json:"target_state"`
		Reason      string           `json:"reason"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		api.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if request.Reason == "" {
		request.Reason = "Manual force scaling via API"
	}

	if err := api.controller.ForceScaling(nodeID, request.TargetState, request.Reason); err != nil {
		api.writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	api.writeJSONResponse(w, http.StatusOK, map[string]string{"message": "Force scaling executed successfully"})
}

// getCloudInstances 获取云实例
func (api *ElasticAPI) getCloudInstances(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	instances, err := api.controller.GetCloudInstances(ctx)
	if err != nil {
		api.writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	api.writeJSONResponse(w, http.StatusOK, instances)
}

// getCloudProviders 获取云服务商列表
func (api *ElasticAPI) getCloudProviders(w http.ResponseWriter, r *http.Request) {
	// 这里简化实现，返回已配置的云服务商名称
	response := map[string]interface{}{
		"message": "Cloud providers endpoint - implementation needed",
	}
	api.writeJSONResponse(w, http.StatusOK, response)
}

// addCloudProvider 添加云服务商
func (api *ElasticAPI) addCloudProvider(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	providerName := vars["providerName"]

	var config CloudProviderConfig
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		api.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if err := api.controller.AddCloudProvider(providerName, &config); err != nil {
		api.writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	api.writeJSONResponse(w, http.StatusOK, map[string]string{"message": "Cloud provider added successfully"})
}

// removeCloudProvider 移除云服务商
func (api *ElasticAPI) removeCloudProvider(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	providerName := vars["providerName"]

	if err := api.controller.RemoveCloudProvider(providerName); err != nil {
		api.writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	api.writeJSONResponse(w, http.StatusOK, map[string]string{"message": "Cloud provider removed successfully"})
}

// writeJSONResponse 写入JSON响应
func (api *ElasticAPI) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

// writeErrorResponse 写入错误响应
func (api *ElasticAPI) writeErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	response := map[string]interface{}{
		"error":     message,
		"timestamp": time.Now().Unix(),
	}
	api.writeJSONResponse(w, statusCode, response)
}
