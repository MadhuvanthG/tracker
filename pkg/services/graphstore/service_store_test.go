package graphstore_test

import (
	"bytes"
	"testing"

	"github.com/deps-cloud/api"
	"github.com/deps-cloud/api/v1alpha/store"
	"github.com/deps-cloud/tracker/pkg/services/graphstore"

	_ "github.com/mattn/go-sqlite3"

	"github.com/stretchr/testify/require"
)

var (
	k1 = []byte("1001")
	k2 = []byte("2002")
	k3 = []byte("3003")
	k4 = []byte("4004")
	k5 = []byte("5005")
	k6 = []byte("6006")
)

func generateData() []byte {
	return make([]byte, 0)
}

func TestNewSQLGraphStore_sqlite(t *testing.T) {
	data := []*store.GraphItem{
		{GraphItemType: "node", K1: k1, K2: k1, Encoding: 0, GraphItemData: generateData()},
		{GraphItemType: "node", K1: k2, K2: k2, Encoding: 0, GraphItemData: generateData()},
		{GraphItemType: "node", K1: k3, K2: k3, Encoding: 0, GraphItemData: generateData()},
		{GraphItemType: "node", K1: k4, K2: k4, Encoding: 0, GraphItemData: generateData()},
		{GraphItemType: "node", K1: k5, K2: k5, Encoding: 0, GraphItemData: generateData()},
		{GraphItemType: "node", K1: k6, K2: k6, Encoding: 0, GraphItemData: generateData()},

		{GraphItemType: "edge", K1: k1, K2: k2, Encoding: 0, GraphItemData: generateData()},
		{GraphItemType: "edge", K1: k2, K2: k3, Encoding: 0, GraphItemData: generateData()},
		{GraphItemType: "edge", K1: k2, K2: k4, Encoding: 0, GraphItemData: generateData()},
		{GraphItemType: "edge", K1: k4, K2: k6, Encoding: 0, GraphItemData: generateData()},
		{GraphItemType: "edge", K1: k3, K2: k5, K3: k1, Encoding: 0, GraphItemData: generateData()},
		{GraphItemType: "edge", K1: k3, K2: k5, K3: k2, Encoding: 0, GraphItemData: generateData()},
	}

	rwdb, rodb, err := graphstore.NewDatabaseConnection("sqlite", "file::memory:?cache=shared", "file::memory:?cache=shared&mode=ro")
	require.Nil(t, err)

	statements, err := graphstore.DefaultStatementsFor("sqlite")
	require.Nil(t, err)

	graphStore, err := graphstore.NewSQLGraphStore(rwdb, rodb, statements)
	require.Nil(t, err)

	_, err = graphStore.Put(nil, &store.PutRequest{
		Items: data,
	})
	require.Nil(t, err)

	response, err := graphStore.List(nil, &store.ListRequest{
		Page:  1,
		Count: 10,
		Type:  "edge",
	})
	require.Nil(t, err)
	require.Len(t, response.Items, 6)

	downstream, err := graphStore.FindDownstream(nil, &store.FindRequest{
		Key:       k2,
		EdgeTypes: []string{"edge"},
	})
	require.Nil(t, err)

	upstream, err := graphStore.FindUpstream(nil, &store.FindRequest{
		Key:       k2,
		EdgeTypes: []string{"edge"},
	})
	require.Nil(t, err)

	require.Len(t, downstream.Pairs, 1)
	require.Len(t, upstream.Pairs, 2)

	require.Equal(t, downstream.Pairs[0].Node.K1, k1)
	require.Equal(t, downstream.Pairs[0].Edge.K1, k1)
	require.Equal(t, downstream.Pairs[0].Edge.K2, k2)

	require.Equal(t, upstream.Pairs[0].Node.K1, k3)
	require.Equal(t, upstream.Pairs[0].Edge.K1, k2)
	require.Equal(t, upstream.Pairs[0].Edge.K2, k3)

	require.Equal(t, upstream.Pairs[1].Node.K1, k4)
	require.Equal(t, upstream.Pairs[1].Edge.K1, k2)
	require.Equal(t, upstream.Pairs[1].Edge.K2, k4)

	// Tests for multiple edges between nodes
	upstreamNodeK3, err := graphStore.FindUpstream(nil, &store.FindRequest{
		Key:       k3,
		EdgeTypes: []string{"edge"},
	})
	require.Nil(t, err)

	require.Len(t, upstreamNodeK3.Pairs, 2)
	require.Equal(t, upstreamNodeK3.Pairs[0].GetEdge().GetK1(), k3)
	require.Equal(t, upstreamNodeK3.Pairs[0].GetEdge().GetK2(), k5)
	require.Equal(t, upstreamNodeK3.Pairs[1].GetEdge().GetK1(), k3)
	require.Equal(t, upstreamNodeK3.Pairs[1].GetEdge().GetK2(), k5)

	edge1k3 := upstreamNodeK3.Pairs[0].GetEdge().GetK3()
	// Must either be equal to k1 or k2
	if !bytes.Equal(edge1k3, k1) {
		require.Equal(t, edge1k3, k2)
	} else {
		require.Equal(t, edge1k3, k1)
	}

	edge2k3 := upstreamNodeK3.Pairs[1].GetEdge().GetK3()
	// Must either be equal to k1 or k2
	if !bytes.Equal(edge2k3, k1) {
		require.Equal(t, edge2k3, k2)
	} else {
		require.Equal(t, edge2k3, k1)
	}

	_, err = graphStore.Delete(nil, &store.DeleteRequest{
		Items: data,
	})
	require.Nil(t, err)
}

func TestReadOnly_sqlite(t *testing.T) {
	_, rodb, err := graphstore.NewDatabaseConnection("sqlite", "", "file::memory:?cache=shared&mode=ro")
	require.Nil(t, err)

	statements, err := graphstore.DefaultStatementsFor("sqlite")
	require.Nil(t, err)

	graphStore, err := graphstore.NewSQLGraphStore(nil, rodb, statements)
	require.Nil(t, err)

	{
		resp, err := graphStore.Put(nil, &store.PutRequest{})
		require.Nil(t, resp)
		require.Equal(t, api.ErrUnsupported, err)
	}

	{
		resp, err := graphStore.Delete(nil, &store.DeleteRequest{})
		require.Nil(t, resp)
		require.Equal(t, api.ErrUnsupported, err)
	}
}

func TestResolveDriverName(t *testing.T) {
	_, err := graphstore.ResolveDriverName("sqlite")
	require.Nil(t, err)

	_, err = graphstore.ResolveDriverName("mysql")
	require.Nil(t, err)

	_, err = graphstore.ResolveDriverName("postgres")
	require.Nil(t, err)

	_, err = graphstore.ResolveDriverName("noDB")
	require.NotNil(t, err)
}
