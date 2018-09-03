package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_data(t *testing.T) {
	assert := assert.New(t)
	assert.NotNil(assert)
	require := require.New(t)
	require.NotNil(require)

	ctx := context.Background()

	dbMySQL, err := newMySQLConnection(ctx)
	require.NoError(err)
	defer func() {
		require.NoError(dbMySQL.Close())
	}()

	txMySQL, err := dbMySQL.BeginTxx(ctx, nil)
	require.NoError(err)
	defer func() {
		if t.Failed() {
			require.NoError(txMySQL.Rollback())
		} else {
			require.NoError(txMySQL.Commit())
		}
	}()

	dbPGSQL, err := newPGSQLConnection()
	require.NoError(err)
	defer func() {
		require.NoError(dbPGSQL.Close())
	}()

	txPGSQL, err := dbPGSQL.BeginTxx(ctx, nil)
	require.NoError(err)
	defer func() {
		if t.Failed() {
			require.NoError(txPGSQL.Rollback())
		} else {
			require.NoError(txPGSQL.Commit())
		}
	}()

	// err = insertData(ctx, dbMySQL, dbPGSQL, 5, 5, 5)
	// require.NoError(err)

	// showMySQLDataCount(ctx, txMySQL, t)
	// showPGSQLDataCount(ctx, txPGSQL, t)

	// selectData(ctx, txMySQL, t, selectMySQLDataSubQuery)
	// selectDataMyAppQuery(ctx, txMySQL, t)

	// selectData(ctx, txPGSQL, t, selectPGSQLDataSubQuery)
	// selectData(ctx, txPGSQL, t, selectPGSQLDataLateralQuery)
	// selectDataPGAppQuery(ctx, txPGSQL, t)
}

func BenchmarkMySQLSelectAppQuery(b *testing.B) {
	assert := assert.New(b)
	assert.NotNil(assert)
	require := require.New(b)
	require.NotNil(require)

	ctx := context.Background()

	dbMySQL, err := newMySQLConnection(ctx)
	require.NoError(err)
	defer func() {
		require.NoError(dbMySQL.Close())
	}()

	txMySQL, err := dbMySQL.BeginTxx(ctx, nil)
	require.NoError(err)
	defer func() {
		if b.Failed() {
			require.NoError(txMySQL.Rollback())
		} else {
			require.NoError(txMySQL.Commit())
		}
	}()

	showMySQLDataCount(ctx, txMySQL, b)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		selectDataMyAppQuery(ctx, txMySQL, b)
	}
}

func BenchmarkMySQLSelectSubQuery(b *testing.B) {
	assert := assert.New(b)
	assert.NotNil(assert)
	require := require.New(b)
	require.NotNil(require)

	ctx := context.Background()

	dbMySQL, err := newMySQLConnection(ctx)
	require.NoError(err)
	defer func() {
		require.NoError(dbMySQL.Close())
	}()

	txMySQL, err := dbMySQL.BeginTxx(ctx, nil)
	require.NoError(err)
	defer func() {
		if b.Failed() {
			require.NoError(txMySQL.Rollback())
		} else {
			require.NoError(txMySQL.Commit())
		}
	}()

	showMySQLDataCount(ctx, txMySQL, b)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		selectData(ctx, txMySQL, b, selectMySQLDataSubQuery)
	}
}

func BenchmarkPGSQLSelectAppQuery(b *testing.B) {
	assert := assert.New(b)
	assert.NotNil(assert)
	require := require.New(b)
	require.NotNil(require)

	ctx := context.Background()

	dbPGSQL, err := newPGSQLConnection()
	require.NoError(err)
	defer func() {
		require.NoError(dbPGSQL.Close())
	}()

	txPGSQL, err := dbPGSQL.BeginTxx(ctx, nil)
	require.NoError(err)
	defer func() {
		if b.Failed() {
			require.NoError(txPGSQL.Rollback())
		} else {
			require.NoError(txPGSQL.Commit())
		}
	}()

	showPGSQLDataCount(ctx, txPGSQL, b)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		selectDataPGAppQuery(ctx, txPGSQL, b)
	}
}

func BenchmarkPGSQLSelectSubQuery(b *testing.B) {
	assert := assert.New(b)
	assert.NotNil(assert)
	require := require.New(b)
	require.NotNil(require)

	ctx := context.Background()

	dbPGSQL, err := newPGSQLConnection()
	require.NoError(err)
	defer func() {
		require.NoError(dbPGSQL.Close())
	}()

	txPGSQL, err := dbPGSQL.BeginTxx(ctx, nil)
	require.NoError(err)
	defer func() {
		if b.Failed() {
			require.NoError(txPGSQL.Rollback())
		} else {
			require.NoError(txPGSQL.Commit())
		}
	}()

	showPGSQLDataCount(ctx, txPGSQL, b)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		selectData(ctx, txPGSQL, b, selectPGSQLDataSubQuery)
	}
}

func BenchmarkPGSQLSelectLateralQuery(b *testing.B) {
	assert := assert.New(b)
	assert.NotNil(assert)
	require := require.New(b)
	require.NotNil(require)

	ctx := context.Background()

	dbPGSQL, err := newPGSQLConnection()
	require.NoError(err)
	defer func() {
		require.NoError(dbPGSQL.Close())
	}()

	txPGSQL, err := dbPGSQL.BeginTxx(ctx, nil)
	require.NoError(err)
	defer func() {
		if b.Failed() {
			require.NoError(txPGSQL.Rollback())
		} else {
			require.NoError(txPGSQL.Commit())
		}
	}()

	showPGSQLDataCount(ctx, txPGSQL, b)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		selectData(ctx, txPGSQL, b, selectPGSQLDataLateralQuery)
	}
}
