package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"regexp"

	"github.com/jmoiron/sqlx"
)

type loggerType interface {
	Logf(format string, args ...interface{})
}

type consoleLoggerType struct{}

func (t consoleLoggerType) Logf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

var consoleLogger loggerType = consoleLoggerType{}

func showDataCounts(logger loggerType, data []selectDataType) {
	forumCount := len(data)
	threadCount := 0
	postCount := 0
	for _, forum := range data {
		threadCount += len(forum.Data.Threads)
		for _, thread := range forum.Data.Threads {
			postCount += len(thread.Posts)
		}
	}
	logger.Logf("forum: %d , thread: %d , post: %d\n", forumCount, threadCount, postCount)
}

func showMySQLDataCount(ctx context.Context, tx *sqlx.Tx, logger loggerType) {
	connURL, err := url.Parse(os.Getenv("MYSQL_URL"))
	if err != nil {
		panic(err)
	}
	reDBName := regexp.MustCompile(`/(\w+)`)
	matches := reDBName.FindAllStringSubmatch(connURL.Opaque, -1)
	dbName := matches[len(matches)-1][1]

	result := struct {
		ForumsCount  int64
		ThreadsCount int64
		PostsCount   int64
	}{}

	if err := tx.GetContext(ctx, &result.ForumsCount, `
SELECT TABLE_ROWS
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = ?
AND TABLE_NAME = 'forums'
	;`, dbName); err != nil {
		panic(err)
	}

	if err := tx.GetContext(ctx, &result.ThreadsCount, `
SELECT TABLE_ROWS
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = ?
AND TABLE_NAME = 'threads'
	;`, dbName); err != nil {
		panic(err)
	}

	if err := tx.GetContext(ctx, &result.PostsCount, `
SELECT TABLE_ROWS
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = ?
AND TABLE_NAME = 'posts'
	;`, dbName); err != nil {
		panic(err)
	}

	logger.Logf("Total %+v\n", result)
}

func showPGSQLDataCount(ctx context.Context, tx *sqlx.Tx, logger loggerType) {
	result := struct {
		ForumsCount  int64
		ThreadsCount int64
		PostsCount   int64
	}{}

	if err := tx.GetContext(ctx, &result.ForumsCount, `
SELECT COUNT(forumID)
FROM forums
	;`); err != nil {
		panic(err)
	}

	if err := tx.GetContext(ctx, &result.ThreadsCount, `
SELECT COUNT(threadID)
FROM threads
	;`); err != nil {
		panic(err)
	}

	if err := tx.GetContext(ctx, &result.PostsCount, `
SELECT COUNT(postID)
FROM posts
	;`); err != nil {
		panic(err)
	}

	logger.Logf("Total %+v\n", result)
}
