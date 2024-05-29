package sync

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/argoproj/argo-cd/v2/util/glob"
	"github.com/argoproj/gitops-engine/pkg/diff"
	jsonpatch "github.com/evanphx/json-patch"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ResourceOverride holds configuration to customize resource diffing and health assessment
type ResourceOverride struct {
	IgnoreDifferences OverrideIgnoreDiff
}

// OverrideIgnoreDiff contains configurations about how fields should be ignored during diffs between
// the desired state and live state
type OverrideIgnoreDiff struct {
	// JSONPointers is a JSON path list following the format defined in RFC4627 (https://datatracker.ietf.org/doc/html/rfc6902#section-3)
	JSONPointers []string
}

// ResourceIgnoreDifferences contains resource filter and list of json paths which should be ignored during comparison with live state.
type ResourceIgnoreDifferences struct {
	Group        string
	Kind         string
	Name         string
	Namespace    string
	JSONPointers []string
}

// ResourceIgnoreDifferences contains resource filter and list of json paths which should be ignored during comparison with live state.
type normalizerPatch interface {
	GetGroupKind() schema.GroupKind
	GetNamespace() string
	GetName() string
	// Apply (un *unstructured.Unstructured) (error)
	Apply(data []byte) ([]byte, error)
}

func getGroupKindForOverrideKey(key string) (string, string, error) {
	var group, kind string
	parts := strings.Split(key, "/")

	if len(parts) == 2 {
		group = parts[0]
		kind = parts[1]
	} else if len(parts) == 1 {
		kind = parts[0]
	} else {
		return "", "", fmt.Errorf("override key must be <group>/<kind> or <kind>, got: '%s' ", key)
	}
	return group, kind, nil
}

type baseNormalizerPatch struct {
	groupKind schema.GroupKind
	namespace string
	name      string
}

func (np *baseNormalizerPatch) GetGroupKind() schema.GroupKind {
	return np.groupKind
}

func (np *baseNormalizerPatch) GetNamespace() string {
	return np.namespace
}

func (np *baseNormalizerPatch) GetName() string {
	return np.name
}

type jsonPatchNormalizerPatch struct {
	baseNormalizerPatch
	patch *jsonpatch.Patch
}

func (np *jsonPatchNormalizerPatch) Apply(data []byte) ([]byte, error) {
	patchedData, err := np.patch.Apply(data)
	if err != nil {
		return nil, err
	}
	return patchedData, nil
}

type ignoreNormalizer struct {
	patches []normalizerPatch
}

// NewIgnoreNormalizer creates diff normalizer which removes ignored fields according to given application spec and resource overrides
func NewIgnoreNormalizer(overrides map[string]ResourceOverride) (diff.Normalizer, error) {
	ignores := make([]ResourceIgnoreDifferences, 0)
	for key, override := range overrides {
		group, kind, err := getGroupKindForOverrideKey(key)
		if err != nil {
			log.Warn(err)
		}
		if len(override.IgnoreDifferences.JSONPointers) > 0 {
			resourceIgnoreDifference := ResourceIgnoreDifferences{
				Group: group,
				Kind:  kind,
			}
			if len(override.IgnoreDifferences.JSONPointers) > 0 {
				resourceIgnoreDifference.JSONPointers = override.IgnoreDifferences.JSONPointers
			}
			ignores = append(ignores, resourceIgnoreDifference)
		}
	}
	patches := make([]normalizerPatch, 0)
	for i := range ignores {
		for _, path := range ignores[i].JSONPointers {
			patchData, err := json.Marshal([]map[string]string{{"op": "remove", "path": path}})
			if err != nil {
				return nil, err
			}
			patch, err := jsonpatch.DecodePatch(patchData)
			if err != nil {
				return nil, err
			}
			patches = append(patches, &jsonPatchNormalizerPatch{
				baseNormalizerPatch: baseNormalizerPatch{
					groupKind: schema.GroupKind{Group: ignores[i].Group, Kind: ignores[i].Kind},
					name:      ignores[i].Name,
					namespace: ignores[i].Namespace,
				},
				patch: &patch,
			})
		}
	}
	return &ignoreNormalizer{patches: patches}, nil
}

// Normalize removes fields from supplied resource using json paths from matching
// items of specified resources ignored differences list
func (n *ignoreNormalizer) Normalize(un *unstructured.Unstructured) error {
	if un == nil {
		return fmt.Errorf("invalid argument: unstructured is nil")
	}
	matched := make([]normalizerPatch, 0)
	for _, patch := range n.patches {
		groupKind := un.GroupVersionKind().GroupKind()

		if glob.Match(patch.GetGroupKind().Group, groupKind.Group) &&
			glob.Match(patch.GetGroupKind().Kind, groupKind.Kind) &&
			(patch.GetName() == "" || patch.GetName() == un.GetName()) &&
			(patch.GetNamespace() == "" || patch.GetNamespace() == un.GetNamespace()) {

			matched = append(matched, patch)
		}
	}
	if len(matched) == 0 {
		return nil
	}

	docData, err := json.Marshal(un)
	if err != nil {
		return err
	}

	for _, patch := range matched {
		patchedDocData, err := patch.Apply(docData)
		if err != nil {
			if shouldLogError(err) {
				log.Debugf("Failed to apply normalization: %v", err)
			}
			continue
		}
		docData = patchedDocData
	}

	err = json.Unmarshal(docData, un)
	if err != nil {
		return err
	}
	return nil
}

func shouldLogError(e error) bool {
	if strings.Contains(e.Error(), "Unable to remove nonexistent key") {
		return false
	}
	if strings.Contains(e.Error(), "remove operation does not apply: doc is missing path") {
		return false
	}
	return true
}

// StateDiffs will apply all required normalizations and calculate the diffs between
// the live and the config/desired states.
func StateDiffs(
	configs, lives []*unstructured.Unstructured,
	overrides map[string]ResourceOverride,
	diffOpts []diff.Option) (*diff.DiffResultList, error) {
	diffNormalizer, err := NewIgnoreNormalizer(overrides)
	if err != nil {
		return nil, fmt.Errorf("failed to create diff normalizer: %w", err)
	}

	diffOpts = append(diffOpts, diff.WithNormalizer(diffNormalizer))

	array, err := diff.DiffArray(configs, lives, diffOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate diff: %w", err)
	}
	return array, nil
}
