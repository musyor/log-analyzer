package model

import "time"

// UnProcessedLog 初始日志/日志时间/唯一ID/是否被处理/处理时间/ai分析ID
// UnProcessedLog 为解析日志
type UnProcessedLog struct {
	ID           string     `json:"id"`        //唯一ID
	Timestamp    time.Time  `json:"timestamp"` // 日志时间
	RawLog       string     `json:"raw_log"`   // 原始日志
	Source       string     `json:"source"`    // 日志源
	LogType      string     `json:"log_type"`
	IndexName    string     `json:"index_name"`
	Processed    bool       `json:"process"`       //是否被处理
	ProcessStamp *time.Time `json:"process_stamp"` // 处理时间
	AnalysisID   *string    `json:"analysis_id"`   //处理ID
}
