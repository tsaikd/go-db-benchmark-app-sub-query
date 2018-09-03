package main

import (
	"context"

	"github.com/jmoiron/sqlx"
)

func selectData(ctx context.Context, tx *sqlx.Tx, logger loggerType, query string) {
	result := []selectDataType{}
	err := tx.SelectContext(ctx, &result, query)
	if err != nil {
		panic(err)
	}
	showDataCounts(logger, result)
}

func selectDataMyAppQuery(ctx context.Context, tx *sqlx.Tx, logger loggerType) {
	result := []selectDataType{}
	if err := tx.SelectContext(ctx, &result, `
SELECT f.forumID AS "forumID", JSON_OBJECT(
	'forumID', f.forumID,
	'name', f.name,
	'lorem', f.lorem,
	'created', f.created
) AS data
FROM forums f
LIMIT 10
	;`); err != nil {
		panic(err)
	}

	for fi := range result {
		forum := &result[fi]
		if err := tx.SelectContext(ctx, &forum.Data.Threads, `
SELECT t.forumID AS "forumID",
	t.threadID AS "threadID",
	t.name,
	t.lorem,
	t.created
FROM threads t
WHERE forumID = ?
LIMIT 10
		;`, forum.ForumID); err != nil {
			panic(err)
		}

		for ti := range forum.Data.Threads {
			thread := &forum.Data.Threads[ti]
			if err := tx.SelectContext(ctx, &thread.Posts, `
SELECT p.threadID AS "threadID",
	p.postID AS "postID",
	p.name,
	p.lorem,
	p.created
FROM posts p
WHERE threadID = ?
LIMIT 10
			;`, thread.ThreadID); err != nil {
				panic(err)
			}
		}
	}
	showDataCounts(logger, result)
}

func selectDataPGAppQuery(ctx context.Context, tx *sqlx.Tx, logger loggerType) {
	result := []selectDataType{}
	if err := tx.SelectContext(ctx, &result, `
SELECT f.forumID AS "forumID", JSON_BUILD_OBJECT(
	'forumID', f.forumID,
	'name', f.name,
	'lorem', f.lorem,
	'created', f.created
) AS data
FROM forums f
LIMIT 10
	;`); err != nil {
		panic(err)
	}

	for fi := range result {
		forum := &result[fi]
		if err := tx.SelectContext(ctx, &forum.Data.Threads, `
SELECT t.forumID AS "forumID",
	t.threadID AS "threadID",
	t.name,
	t.lorem,
	t.created
FROM threads t
WHERE forumID = $1
LIMIT 10
		;`, forum.ForumID); err != nil {
			panic(err)
		}

		for ti := range forum.Data.Threads {
			thread := &forum.Data.Threads[ti]
			if err := tx.SelectContext(ctx, &thread.Posts, `
SELECT p.threadID AS "threadID",
	p.postID AS "postID",
	p.name,
	p.lorem,
	p.created
FROM posts p
WHERE threadID = $1
LIMIT 10
			;`, thread.ThreadID); err != nil {
				panic(err)
			}
		}
	}
	showDataCounts(logger, result)
}

const selectMySQLDataSubQuery = `
SELECT f.forumID, JSON_OBJECT(
	'forumID', f.forumID,
	'name', f.name,
	'lorem', f.lorem,
	'created', f.created,
	'threads', t.threads
) AS data
FROM forums f
INNER JOIN (
	SELECT t.forumID, CAST(CONCAT(
		'[',
		GROUP_CONCAT(
			JSON_OBJECT(
				'forumID', t.forumID,
				'threadID', t.threadID,
				'name', t.name,
				'lorem', t.lorem,
				'created', t.created,
				'posts', p2.posts
			)
		),
		']'
	) AS JSON) AS threads
	FROM (
		SELECT @trnum := CASE
			WHEN @forumID = forumID THEN @trnum + 1
			ELSE 1
			END AS trnum,
			@forumID := forumID AS forumID,
			threadID,
			name,
			lorem,
			created
		FROM threads, (SELECT @trnum:=0, @forumID:='') as tt
		ORDER BY forumID
	) t
	INNER JOIN (
		SELECT p.threadID, CAST(CONCAT(
			'[',
			GROUP_CONCAT(
				JSON_OBJECT(
					'threadID', p.threadID,
					'postID', p.postID,
					'name', p.name,
					'lorem', p.lorem,
					'created', p.created
				)
			),
			']'
		) AS JSON) AS posts
		FROM (
			SELECT @prnum := CASE
				WHEN @threadID = threadID THEN @prnum + 1
				ELSE 1
				END AS prnum,
				@threadID := threadID AS threadID,
				postID,
				name,
				lorem,
				created
			FROM posts, (SELECT @prnum:=0, @threadID:='') as pt
			ORDER BY threadID
		) p
		WHERE p.prnum <= 10
		GROUP BY threadID
	) p2 USING (threadID)
	WHERE t.trnum <= 10
	GROUP BY forumID
) t USING (forumID)
LIMIT 10
;`

const selectPGSQLDataSubQuery = `
SELECT f.forumID AS "forumID", JSON_BUILD_OBJECT(
	'forumID', f.forumID,
	'name', f.name,
	'lorem', f.lorem,
	'created', f.created,
	'threads', t.threads
) AS data
FROM forums f
INNER JOIN (
	SELECT t.forumID, JSON_AGG(JSON_BUILD_OBJECT(
		'forumID', t.forumID,
		'threadID', t.threadID,
		'name', t.name,
		'lorem', t.lorem,
		'created', t.created,
		'posts', p.posts
	)) AS threads
	FROM (
		SELECT forumID, threadID, name, lorem, created, ROW_NUMBER() OVER (PARTITION BY forumID) AS rnum
		FROM threads
	) t
	INNER JOIN (
		SELECT p.threadID, JSON_AGG(JSON_BUILD_OBJECT(
			'threadID', p.threadID,
			'postID', p.postID,
			'name', p.name,
			'lorem', p.lorem,
			'created', p.created
		)) AS posts
		FROM (
			SELECT threadID, postID, name, lorem, created, ROW_NUMBER() OVER (PARTITION BY threadID) AS rnum
			FROM posts
		) p
		WHERE p.rnum <= 10
		GROUP BY threadID
	) p USING (threadID)
	WHERE t.rnum <= 10
	GROUP BY forumID
) t USING (forumID)
LIMIT 10
;`

const selectPGSQLDataLateralQuery = `
SELECT f.forumID AS "forumID", JSON_BUILD_OBJECT(
	'forumID', f.forumID,
	'name', f.name,
	'lorem', f.lorem,
	'created', f.created,
	'threads', JSON_AGG(t2.thread)
) AS data
FROM forums f
JOIN LATERAL (
	SELECT t.forumID, t.threadID, JSON_BUILD_OBJECT(
		'forumID', t.forumID,
		'threadID', t.threadID,
		'name', t.name,
		'lorem', t.lorem,
		'created', t.created,
		'posts', JSON_AGG(p2.post)
	) AS thread
	FROM threads t
	JOIN LATERAL (
		SELECT p.threadID,
			p.postID,
			JSON_BUILD_OBJECT(
				'threadID', p.threadID,
				'postID', p.postID,
				'name', p.name,
				'lorem', p.lorem,
				'created', p.created
			) AS post
		FROM posts p
		WHERE p.threadID = t.threadID
		LIMIT 10
	) p2
	ON TRUE
	WHERE t.forumID = f.forumID
	GROUP BY t.forumID, t.threadID
	LIMIT 10
) t2
ON TRUE
GROUP BY f.forumID
LIMIT 10
;`
