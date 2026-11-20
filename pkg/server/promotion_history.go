package server

import (
	"encoding/json"
	"strconv"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kargoapi "github.com/akuity/kargo/api/v1alpha1"
)

// promotionHistoryRow omits execution details but keeps the step references and
// outputs used by the history table's pull-request and output-link plugins.
func promotionHistoryRow(p *kargoapi.Promotion) kargoapi.Promotion {
	row := kargoapi.Promotion{
		TypeMeta: p.TypeMeta,
		ObjectMeta: metav1.ObjectMeta{
			Name:              p.Name,
			Namespace:         p.Namespace,
			UID:               p.UID,
			CreationTimestamp: p.CreationTimestamp,
			ResourceVersion:   p.ResourceVersion,
			Annotations:       map[string]string{},
		},
		Spec:   kargoapi.PromotionSpec{Stage: p.Spec.Stage, Freight: p.Spec.Freight, Steps: []kargoapi.PromotionStep{}},
		Status: kargoapi.PromotionStatus{Phase: p.Status.Phase, Message: p.Status.Message},
	}
	for _, key := range []string{
		kargoapi.AnnotationKeyAbort,
		kargoapi.AnnotationKeyCreateActor,
		kargoapi.AnnotationKeyRollback,
	} {
		if v, ok := p.Annotations[key]; ok {
			row.Annotations[key] = v
		}
	}
	if p.Status.State == nil {
		return row
	}
	var state map[string]json.RawMessage
	if json.Unmarshal(p.Status.State.Raw, &state) != nil {
		return row
	}
	outputs := map[string]map[string]json.RawMessage{}
	for i, step := range p.Spec.Steps {
		alias := step.As
		// Match getPromotionStepAlias for unaliased steps in the UI.
		if alias == "" {
			alias = "step-" + strconv.Itoa(i)
		}
		raw := state[alias]
		if len(raw) == 0 {
			continue
		}
		var output struct {
			Links json.RawMessage `json:"links"`
			PR    json.RawMessage `json:"pr"`
		}
		if json.Unmarshal(raw, &output) != nil {
			continue
		}
		links := map[string]json.RawMessage{}
		if len(output.Links) > 0 {
			links["links"] = output.Links
		}
		if len(output.PR) > 0 && (step.Uses == "git-open-pr" || step.Uses == "git-wait-for-pr") {
			links["pr"] = output.PR
		}
		if len(links) == 0 {
			continue
		}
		row.Spec.Steps = append(row.Spec.Steps, kargoapi.PromotionStep{As: alias, Uses: step.Uses})
		outputs[alias] = links
	}
	if len(outputs) > 0 {
		raw, err := json.Marshal(outputs)
		if err == nil {
			row.Status.State = &apiextensionsv1.JSON{Raw: raw}
		}
	}
	return row
}
