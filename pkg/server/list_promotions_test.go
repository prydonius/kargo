package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kargoapi "github.com/akuity/kargo/api/v1alpha1"
	"github.com/akuity/kargo/pkg/server/config"
)

func Test_server_listPromotions(t *testing.T) {
	testProject := &kargoapi.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "fake-project"},
	}
	testRESTEndpoint(
		t, &config.ServerConfig{},
		http.MethodGet, "/v1beta1/projects/"+testProject.Name+"/promotions",
		[]restTestCase{
			{
				name: "Project does not exist",
				assertions: func(t *testing.T, w *httptest.ResponseRecorder, _ client.Client) {
					require.Equal(t, http.StatusNotFound, w.Code)
				},
			},
			{
				name:          "no Promotions exist",
				clientBuilder: fake.NewClientBuilder().WithObjects(testProject),
				assertions: func(t *testing.T, w *httptest.ResponseRecorder, _ client.Client) {
					require.Equal(t, http.StatusOK, w.Code)
					promos := &kargoapi.PromotionList{}
					err := json.Unmarshal(w.Body.Bytes(), promos)
					require.NoError(t, err)
					require.Empty(t, promos.Items)
				},
			},
			{
				name: "lists Promotions",
				clientBuilder: fake.NewClientBuilder().WithObjects(
					testProject,
					&kargoapi.Promotion{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: testProject.Name,
							Name:      "promotion-1",
						},
					},
					&kargoapi.Promotion{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: testProject.Name,
							Name:      "promotion-2",
						},
					},
				),
				assertions: func(t *testing.T, w *httptest.ResponseRecorder, _ client.Client) {
					require.Equal(t, http.StatusOK, w.Code)

					// Examine the Promotions in the response
					promos := &kargoapi.PromotionList{}
					err := json.Unmarshal(w.Body.Bytes(), promos)
					require.NoError(t, err)
					require.Len(t, promos.Items, 2)
				},
			},
		},
	)
}

func Test_server_listPromotions_watch(t *testing.T) {
	const projectName = "fake-project"

	testRESTWatchEndpoint(
		t, &config.ServerConfig{},
		"/v1beta1/projects/"+projectName+"/promotions?watch=true",
		[]restWatchTestCase{
			{
				name:          "project not found",
				url:           "/v1beta1/projects/non-existent/promotions?watch=true",
				clientBuilder: fake.NewClientBuilder(),
				assertions: func(t *testing.T, w *httptest.ResponseRecorder, _ client.Client) {
					require.Equal(t, http.StatusNotFound, w.Code)
				},
			},
			{
				name: "watches all promotions successfully",
				clientBuilder: fake.NewClientBuilder().WithObjects(
					&kargoapi.Project{
						ObjectMeta: metav1.ObjectMeta{Name: projectName},
					},
					&kargoapi.Promotion{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: projectName,
							Name:      "promotion-1",
						},
					},
					&kargoapi.Promotion{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: projectName,
							Name:      "promotion-2",
						},
					},
				),
				operations: func(ctx context.Context, c client.Client) {
					// Create a new promotion to trigger a watch event
					newPromo := &kargoapi.Promotion{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: projectName,
							Name:      "promotion-3",
						},
					}
					_ = c.Create(ctx, newPromo)
				},
				assertions: func(t *testing.T, w *httptest.ResponseRecorder, _ client.Client) {
					require.Equal(t, http.StatusOK, w.Code)

					// Verify SSE headers
					require.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
					require.Equal(t, "no-cache", w.Header().Get("Cache-Control"))
					require.Equal(t, "keep-alive", w.Header().Get("Connection"))

					// The response body should contain SSE events from the create operation
					body := w.Body.String()
					require.Contains(t, body, "data:")
				},
			},
			{
				name: "watches empty promotion list",
				clientBuilder: fake.NewClientBuilder().WithObjects(
					&kargoapi.Project{
						ObjectMeta: metav1.ObjectMeta{Name: projectName},
					},
				),
				assertions: func(t *testing.T, w *httptest.ResponseRecorder, _ client.Client) {
					require.Equal(t, http.StatusOK, w.Code)

					// Verify SSE headers are set
					require.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
				},
			},
		},
	)
}

func Test_server_promotionHistory(t *testing.T) {
	const ns = "project"
	const stage = "staging"
	const path = "/v1beta1/projects/project/promotions?stage=staging&summary=true"
	project := &kargoapi.Project{ObjectMeta: metav1.ObjectMeta{Name: ns}}
	p := &kargoapi.Promotion{}
	require.NoError(t, json.Unmarshal([]byte(`{
 "metadata": {
   "name": "example", "namespace": "project", "labels": {"kargo.akuity.io/stage": "staging"},
   "annotations": {"kargo.akuity.io/create-actor": "admin", "irrelevant": "omit-me"}
 },
 "spec": {"stage": "staging", "freight": "freight", "steps": [{"uses": "git-open-pr", "config": {"x": "omit-me"}}]},
 "status": {"phase": "Succeeded", "state": {"step-0": {
   "pr": {"url": "https://example.org/pull/1"}, "links": [{"url": "https://example.org/log"}], "large": "omit-me"
 }}}
}`), p))
	other := p.DeepCopy()
	other.Name = "other"
	other.Labels[kargoapi.LabelKeyStage] = "other"
	other.Spec.Stage = "other"
	check := func(t *testing.T, row *kargoapi.Promotion) {
		require.Equal(t, p.Name, row.Name)
		require.Equal(t, stage, row.Spec.Stage)
		require.Equal(t, p.Spec.Freight, row.Spec.Freight)
		require.NotEmpty(t, row.ResourceVersion)
		require.Equal(t, p.Status.Phase, row.Status.Phase)
		require.Equal(t, "admin", row.Annotations[kargoapi.AnnotationKeyCreateActor])
		require.Equal(t, []kargoapi.PromotionStep{{As: "step-0", Uses: "git-open-pr"}}, row.Spec.Steps)
		require.NotNil(t, row.Status.State)
		require.JSONEq(t, `{"step-0": {
 "pr": {"url":"https://example.org/pull/1"},
 "links": [{"url":"https://example.org/log"}]
}}`, string(row.Status.State.Raw))
	}
	testRESTEndpoint(t, &config.ServerConfig{}, http.MethodGet, path, []restTestCase{{
		name:          "seed",
		clientBuilder: fake.NewClientBuilder().WithObjects(project.DeepCopy(), p.DeepCopy(), other.DeepCopy()),
		assertions: func(t *testing.T, w *httptest.ResponseRecorder, _ client.Client) {
			require.Equal(t, http.StatusOK, w.Code)
			require.NotContains(t, w.Body.String(), "omit-me")
			var list kargoapi.PromotionList
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &list))
			require.Len(t, list.Items, 1)
			check(t, &list.Items[0])
		},
	}})
	testRESTWatchEndpoint(t, &config.ServerConfig{}, path+"&watch=true", []restWatchTestCase{{
		name:          "watch",
		clientBuilder: fake.NewClientBuilder().WithObjects(project),
		operations: func(ctx context.Context, c client.Client) {
			for _, promo := range []*kargoapi.Promotion{other, p} {
				if err := c.Create(ctx, promo.DeepCopy()); err != nil {
					t.Errorf("creating test Promotion: %v", err)
					return
				}
			}
		},
		assertions: func(t *testing.T, w *httptest.ResponseRecorder, _ client.Client) {
			require.Equal(t, http.StatusOK, w.Code)
			require.NotContains(t, w.Body.String(), "omit-me")
			for _, line := range strings.Split(w.Body.String(), "\n") {
				if data, ok := strings.CutPrefix(line, "data: "); ok {
					var e WatchEvent[kargoapi.Promotion]
					require.NoError(t, json.Unmarshal([]byte(data), &e))
					check(t, &e.Object)
					return
				}
			}
			t.Fatal("no watch event")
		},
	}})
}
