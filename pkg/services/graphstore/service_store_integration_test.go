package graphstore_test

import (
	"bytes"
	"net"
	"net/url"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/deps-cloud/api/v1alpha/store"
	"github.com/deps-cloud/tracker/pkg/services/graphstore"
	_ "github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/ory/dockertest"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	pgURL        *url.URL
	rwPostgresDb *sqlx.DB
	roPostgresDb *sqlx.DB
)

func TestMain(m *testing.M) {
	code := 0
	defer func() {
		os.Exit(code)
	}()

	pgURL = &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword("user", "password"),
		Path:   "depscloustest",
	}
	q := pgURL.Query()
	q.Add("sslmode", "disable")
	pgURL.RawQuery = q.Encode()

	pool, err := dockertest.NewPool("")
	if err != nil {
		logrus.Error("Could not connect to docker")
	}

	pw, _ := pgURL.User.Password()
	runOpts := dockertest.RunOptions{
		Repository:   "postgres",
		Tag:          "12.3",
		ExposedPorts: []string{"5432"},
		Env: []string{
			"POSTGRES_USER=" + pgURL.User.Username(),
			"POSTGRES_PASSWORD=" + pw,
			"POSTGRES_DB=" + pgURL.Path,
		},
	}

	resource, err := pool.RunWithOptions(&runOpts)
	if err != nil {
		logrus.Errorf("Could not start postgres container with %s", err.Error())
	}
	defer func() {
		err = pool.Purge(resource)
		rwPostgresDb = nil
		roPostgresDb = nil
		if err != nil {
			logrus.Error("Could not purge resource")
		}
	}()

	// TODO: Need to verify that this works if we run this on a different OS (ex. in our CI pipeline)
	pgURL.Host = resource.Container.NetworkSettings.IPAddress

	// Docker layer network is different on Mac
	if runtime.GOOS == "darwin" {
		pgURL.Host = net.JoinHostPort(resource.GetBoundIP("5432/tcp"), resource.GetPort("5432/tcp"))
	}

	pool.MaxWait = 10 * time.Second
	err = pool.Retry(func() error {
		db, err := sqlx.Open("pgx", pgURL.String())
		if err != nil {
			return err
		}
		rwPostgresDb = db
		roPostgresDb = db
		return db.Ping()
	})
	if err != nil {
		logrus.Error("Could not connect to postgres server")
	}

	code = m.Run()
}

func TestNewSQLGraphStore_postgres(t *testing.T) {
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

	require.NotNil(t, rwPostgresDb)
	require.NotNil(t, roPostgresDb)

	statements, err := graphstore.DefaultStatementsFor("postgres")
	require.Nil(t, err)

	graphStoreServer, err := graphstore.NewSQLGraphStore(rwPostgresDb, roPostgresDb, statements)
	require.Nil(t, err)

	_, err = graphStoreServer.Put(nil, &store.PutRequest{
		Items: data,
	})
	require.Nil(t, err)

	response, err := graphStoreServer.List(nil, &store.ListRequest{
		Page:  1,
		Count: 10,
		Type:  "edge",
	})
	require.Nil(t, err)
	require.Len(t, response.Items, 6)

	downstream, err := graphStoreServer.FindDownstream(nil, &store.FindRequest{
		Key:       k2,
		EdgeTypes: []string{"edge"},
	})
	require.Nil(t, err)

	upstream, err := graphStoreServer.FindUpstream(nil, &store.FindRequest{
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
	upstreamNodeK3, err := graphStoreServer.FindUpstream(nil, &store.FindRequest{
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

	_, err = graphStoreServer.Delete(nil, &store.DeleteRequest{
		Items: data,
	})
	require.Nil(t, err)
}
