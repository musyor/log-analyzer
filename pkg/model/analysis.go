package model

import "time"

const (
	ReviewStatusPending  = "pending"
	ReviewStatusApproved = "approve"
	ReviewStatusReject   = "reject"
)

type AnalysisResult struct {
	ID                string           `json:"id"`
	LogID             string           `json:"log_id"`
	RawLog            string           `json:"raw_log"`
	LogType           string           `json:"log_type"`
	TimeStamp         time.Time        `json:"time_stamp"`
	SuggestionFilter  string           `json:"suggestion_filter"`
	MatchingCondition string           `json:"matching_condition"`
	FilterValidation  FilterValidation `json:"filter_validation"`
	ReviewStatus      string           `json:"review_status"`
	Reviewer          *string          `json:"reviewer"`
	ReviewTimeStamp   *time.Time       `json:"review_time_stamp"`
	ReviewComments    string           `json:"review_comments"`
	IsImplemented     bool             `json:"is_implemented"`
}

type FilterValidation struct {
	IsValid          bool   `json:"is_valid"`
	ValidationDetail string `json:"validation_detail"`
}
