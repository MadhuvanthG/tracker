package services

import (
	"crypto/sha256"
	"strings"

	"github.com/deps-cloud/api/v1alpha/schema"
	"github.com/deps-cloud/api/v1alpha/store"
	"github.com/deps-cloud/tracker/pkg/services/graphstore"
)

func key(vars ...string) []byte {
	hash := sha256.New()
	for _, val := range vars {
		hash.Write([]byte(val))
	}
	return hash.Sum(nil)
}

func keyForSource(source *schema.Source) []byte {
	return key(source.GetUrl())
}

func keyForModule(module *schema.Module) []byte {
	return key(module.GetLanguage(), module.GetOrganization(), module.GetModule())
}

func readableKey(item *store.GraphItem) string {
	keys := []string{
		item.GetGraphItemType(),
		graphstore.Base64encode(item.GetK1()),
		graphstore.Base64encode(item.GetK2()),
	}

	if k3 := item.GetK3(); len(k3) > 0 {
		keys = append(keys, graphstore.Base64encode(k3))
	}

	return strings.Join(keys, "---")
}
