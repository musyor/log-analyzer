package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log-analyzer/pkg/es"
	"log-analyzer/pkg/model"
	"time"
)

type Collector interface {
	FetchUnprocessedLogs(ctx context.Context, batchSize int) ([]model.UnProcessedLog, error)
	UpdateLogProcessStatus(ctx context.Context, logID string) error
	UpdateLogAnalysisStatus(ctx context.Context, logID string, analysisID string) error
}

type ESCollector struct {
	esClient  es.Client
	indexName string
}

func (e *ESCollector) FetchUnprocessedLogs(ctx context.Context, batchSize int) ([]model.UnProcessedLog, error) {
	// 构建查询，获取未处理的日志
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"processed": false,
						},
					},
				},
				"must_not": []map[string]interface{}{
					{
						"exists": map[string]interface{}{
							"field": "processing_timestamp",
						},
					},
				},
			},
		},
		"sort": []map[string]interface{}{
			{
				"timestamp": map[string]interface{}{
					"order": "desc",
				},
			},
		},
	}

	if batchSize <= 0 {
		batchSize = model.DefaultBatchSize
	} else if batchSize > model.MaxBatchSize {
		batchSize = model.MaxBatchSize
	}
	result, err := e.esClient.Search(ctx, e.indexName, query, batchSize)
	if err != nil {
		return nil, err
	}

	logs := make([]model.UnProcessedLog, 0, len(result))
	for _, m := range result {
		var log model.UnProcessedLog
		resultBytes, err := json.Marshal(m)
		if err != nil {
			continue
		}

		err = json.Unmarshal(resultBytes, &log)
		if err != nil {
			continue
		}

		if id, ok := m["_id"].(string); ok {
			log.ID = id
		}
		logs = append(logs, log)
	}
	return logs, err
}

func (e *ESCollector) UpdateLogProcessStatus(ctx context.Context, logID string) error {
	now := time.Now()
	update := map[string]interface{}{
		"processing_timestamp": now,
	}

	err := e.esClient.Update(ctx, e.indexName, logID, update)
	if err != nil {
		return fmt.Errorf("failed to mark log as processing: %w", err)
	}
	return nil
}

func (e *ESCollector) UpdateLogAnalysisStatus(ctx context.Context, logID string, analysisID string) error {
	update := map[string]interface{}{
		"processed":   "true",
		"analysis_id": analysisID,
	}
	err := e.esClient.Update(ctx, e.indexName, logID, update)
	if err != nil {
		return fmt.Errorf("failed to update log with analysis: %w", err)
	}
	return nil
}
