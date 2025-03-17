package es

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"net/http"
	"strings"
	"time"
)

type Client interface {
	// IsConnected 连接相关
	IsConnected() bool

	// CreatNewIndex 新增索引
	CreatNewIndex(ctx context.Context, index string, mapping string) error
	// IsExistIndex 索引是否存在
	IsExistIndex(ctx context.Context, index string) (bool, error)

	// Search  查询条数
	Search(ctx context.Context, index string, query map[string]interface{}, size int) ([]map[string]interface{}, error)
	// GetByID 根据ID获取文档
	GetByID(ctx context.Context, index string, ID string) (map[string]interface{}, error)
	// Create 创建文档
	Create(ctx context.Context, index string, id string, document interface{}) error
	// Update 更新文档
	Update(ctx context.Context, index string, ID string, document interface{}) error
}

// ESClient 实现Client接口
type ESClient struct {
	Client *elasticsearch.Client
}

// NewESClient 配置单独设置/做了账号密码可缺失/客户端状态确认/响应状态确认
// NewESClient 创建ES客户端
func NewESClient(address []string, username string, password string) (*ESClient, error) {

	cfg := elasticsearch.Config{
		Addresses: address,
		Transport: &http.Transport{
			ResponseHeaderTimeout: 10 * time.Second,
		},
	}

	if username != "" && password != "" {
		cfg.Username = username
		cfg.Password = password
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("客户端创建失败:%s", err)
	}

	response, err := client.Info()
	if err != nil {
		return nil, fmt.Errorf("ES发送信息确认失败:%s", err)
	}
	defer response.Body.Close()

	if response.IsError() {
		return nil, fmt.Errorf("ES返回有无:%s", response.String())
	}

	return &ESClient{Client: client}, err
}

func (e *ESClient) IsConnected() bool {
	response, err := e.Client.Info()
	if err != nil {
		return false
	}
	defer response.Body.Close()
	return response.IsError() == false
}

func (e *ESClient) CreatNewIndex(ctx context.Context, index string, mapping string) error {

	exist, err := e.IsExistIndex(ctx, index)
	if err != nil {
		return err
	}

	if exist {
		return fmt.Errorf("索引已经存在")
	}

	resp, err := e.Client.Indices.Create(
		index,
		e.Client.Indices.Create.WithBody(strings.NewReader(mapping)),
	)
	if err != nil {
		return fmt.Errorf("创建索引请求失败: %w", err)
	}
	if resp.IsError() {
		return fmt.Errorf("创建索引失败: %s", resp.String())
	}

	return nil
}

func (e *ESClient) IsExistIndex(ctx context.Context, index string) (bool, error) {
	resp, err := e.Client.Indices.Exists([]string{index})
	if err != nil {
		return false, err
	}
	resp.Body.Close()
	return resp.StatusCode == 200, err
}

func (e *ESClient) Search(ctx context.Context, index string, query map[string]interface{}, size int) ([]map[string]interface{}, error) {
	buf := new(bytes.Buffer)
	if query == nil {
		query = map[string]interface{}{
			"query": map[string]interface{}{
				"match_all": map[string]interface{}{},
			},
		}
	}
	if err := json.NewEncoder(buf).Encode(query); err != nil {
		return nil, err
	}

	if size < 0 {
		size = 10
	}
	response, err := e.Client.Search(
		e.Client.Search.WithIndex(index),
		e.Client.Search.WithBody(buf),
		e.Client.Search.WithSize(size),
		e.Client.Search.WithContext(ctx),
	)
	if err != nil {
		return nil, err
	}

	if response.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(response.Body).Decode(&e); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("查询失败:%v", e)
	}

	// 解析响应
	var r map[string]interface{}
	if err := json.NewDecoder(response.Body).Decode(&r); err != nil {
		return nil, err
	}

	hits, found := r["hits"].(map[string]interface{})
	if !found {
		return []map[string]interface{}{}, err
	}
	hitsHist, found := hits["hits"].(map[string]interface{})
	if !found {
		return []map[string]interface{}{}, err
	}

	var result []map[string]interface{}
	for _, hit := range hitsHist {
		hitMap, ok := hit.(map[string]interface{})
		if !ok {
			continue
		}

		source, found := hitMap["_source"].(map[string]interface{})
		if !found {
			continue
		}

		if id, found := hitMap["_id"]; found {
			source["_id"] = id
		}
		result = append(result, source)
	}
	return result, nil
}

func (e *ESClient) GetByID(ctx context.Context, index string, ID string) (map[string]interface{}, error) {
	resp, err := e.Client.Get(index, ID, e.Client.Get.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == 404 {
		return nil, errors.New("document not found")
	}
	if resp.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("elasticsearch error: %v", e)
	}
	defer resp.Body.Close()

	var r map[string]interface{}

	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, err
	}

	source, found := r["_resource"].(map[string]interface{})
	if !found {
		return nil, errors.New("_source not found in response")
	}

	source["_id"] = r["_id"]

	return source, nil
}

func (e *ESClient) Create(ctx context.Context, index string, id string, document interface{}) error {
	jsonDocument, err := json.Marshal(document)
	if err != nil {
		return err
	}
	esreq := esapi.CreateRequest{
		Index:      index,
		DocumentID: id,
		Body:       bytes.NewReader(jsonDocument),
		Refresh:    "true",
	}
	resp, err := esreq.Do(ctx, e.Client)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
			return err
		}
		return fmt.Errorf("elasticsearch error: %v", e)
	}
	return nil
}

func (e *ESClient) Update(ctx context.Context, index string, ID string, document interface{}) error {
	updateDoc := map[string]interface{}{
		"doc": document,
	}
	jsonData, err := json.Marshal(updateDoc)
	if err != nil {
		return err
	}
	req := esapi.UpdateRequest{
		Index:      index,
		DocumentID: ID,
		Body:       bytes.NewReader(jsonData),
		Refresh:    "true"}

	resp, err := req.Do(ctx, e.Client)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
			return err
		}
		return fmt.Errorf("elasticsearch error: %v", e)
	}

	return nil
}
