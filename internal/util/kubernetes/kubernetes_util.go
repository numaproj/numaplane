package kubernetes

import (
	"fmt"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// this file is designed to hold utility functions for Kubernetes that are not specific to
// any type of resource

// validManifestExtensions contains the supported extension for raw file.
var validManifestExtensions = map[string]struct{}{"yaml": {}, "yml": {}, "json": {}}

func IsValidKubernetesNamespace(name string) bool {
	// All namespace names must be valid RFC 1123 DNS labels.
	errs := validation.IsDNS1123Label(name)
	reservedNamesRegex := regexp.MustCompile(`^(kubernetes-|kube-)`)
	if len(errs) == 0 && !reservedNamesRegex.MatchString(name) {
		return true
	}
	return false
}

func IsValidKubernetesManifestFile(fileName string) bool {
	fileExt := strings.Split(fileName, ".")
	if _, ok := validManifestExtensions[fileExt[len(fileExt)-1]]; ok {
		return true
	}
	return false
}

// ResourceFilter filter resources based on allowed Resource Types
type ResourceFilter struct {
	IncludedResources []ResourceType
}

type ResourceType struct {
	Group string
	Kind  string
}

func (n *ResourceFilter) IsExcludedResource(group, kind, _ string) bool {
	for _, resource := range n.IncludedResources {
		if resource.Kind == "" {
			// When Kind is empty, we only check if Group matches
			if group == resource.Group {
				return false
			}
		} else if group == resource.Group && kind == resource.Kind {
			return false
		}
	}
	return true
}

// ParseResourceFilter parse the given rules to generate the
// list of resources we allow or watch. The rules are delimited by ';', and
// each rule is composed by both 'group' and 'kind' which can be empty.
// For example, 'group=apps,kind=Deployment;group=,kind=ConfigMap'.
// Note that empty rule is valid.
func ParseResourceFilter(rules string) ([]ResourceType, error) {
	filteredResources := make([]ResourceType, 0)
	if rules == "" {
		return filteredResources, nil
	}
	rulesArr := strings.Split(rules, ";")
	err := fmt.Errorf("malformed resource filter rules %q", rules)
	for _, rule := range rulesArr {
		ruleArr := strings.Split(rule, ",")
		if len(ruleArr) != 2 {
			return nil, err
		}
		groupArr := strings.Split(ruleArr[0], "=")
		kindArr := strings.Split(ruleArr[1], "=")
		if !strings.EqualFold(groupArr[0], "group") || !strings.EqualFold(kindArr[0], "kind") {
			return nil, err
		}
		filteredResource := ResourceType{
			Group: groupArr[1],
			Kind:  kindArr[1],
		}
		filteredResources = append(filteredResources, filteredResource)
	}
	return filteredResources, nil

}
