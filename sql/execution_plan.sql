--Q1
\c pg1

EXPLAIN SELECT COUNT(P.tags) AS CountTags, P.tags, AVG(C.CreationDate - P.CreationDate) AS AvgAnswerTime, AVG(P.Score)::INTEGER AvgScore
FROM posts.posts AS P JOIN posts.Comments AS C ON P.Id = C.PostId
WHERE tags LIKE '%|postgresql|%' AND LENGTH(tags) - LENGTH(REPLACE(tags, '|', '')) = 3 
GROUP BY tags
ORDER BY CountTags DESC
LIMIT 5;

EXPLAIN ANALYZE SELECT COUNT(P.tags) AS CountTags, P.tags, AVG(C.CreationDate - P.CreationDate) AS AvgAnswerTime, AVG(P.Score)::INTEGER AvgScore
FROM posts.posts AS P JOIN posts.Comments AS C ON P.Id = C.PostId
WHERE tags LIKE '%|postgresql|%' AND LENGTH(tags) - LENGTH(REPLACE(tags, '|', '')) = 3 
GROUP BY tags
ORDER BY CountTags DESC
LIMIT 5;

CREATE INDEX index_tags ON posts.posts(tags);

EXPLAIN SELECT COUNT(P.tags) AS CountTags, P.tags, AVG(C.CreationDate - P.CreationDate) AS AvgAnswerTime, AVG(P.Score)::INTEGER AvgScore
FROM posts.posts AS P JOIN posts.Comments AS C ON P.Id = C.PostId
WHERE tags LIKE '%|postgresql|%' AND LENGTH(tags) - LENGTH(REPLACE(tags, '|', '')) = 3 
GROUP BY tags
ORDER BY CountTags DESC
LIMIT 5;

EXPLAIN ANALYZE SELECT COUNT(P.tags) AS CountTags, P.tags, AVG(C.CreationDate - P.CreationDate) AS AvgAnswerTime, AVG(P.Score)::INTEGER AvgScore
FROM posts.posts AS P JOIN posts.Comments AS C ON P.Id = C.PostId
WHERE tags LIKE '%|postgresql|%' AND LENGTH(tags) - LENGTH(REPLACE(tags, '|', '')) = 3 
GROUP BY tags
ORDER BY CountTags DESC
LIMIT 5;

--Q2
EXPLAIN SELECT DisplayName AS dname, Score FROM Users.Users U
JOIN Posts.Posts P ON U.id = P.OwnerUserId
WHERE AcceptedAnswerId IS NOT NULL 
AND Score IN (SELECT Score FROM posts.posts
              WHERE AcceptedAnswerId IS NOT NULL 
              GROUP BY Score
              HAVING Score < 0) 
AND Tags LIKE '%|postgresql|%'
ORDER BY Score
LIMIT 5;

EXPLAIN ANALYZE SELECT DisplayName AS dname, Score FROM Users.Users U
JOIN Posts.Posts P ON U.id = P.OwnerUserId
WHERE AcceptedAnswerId IS NOT NULL 
AND Score IN (SELECT Score FROM posts.posts
              WHERE AcceptedAnswerId IS NOT NULL 
              GROUP BY Score
              HAVING Score < 0) 
AND Tags LIKE '%|postgresql|%'
ORDER BY Score
LIMIT 5;

CREATE INDEX index_score ON posts.posts(Score);

EXPLAIN SELECT DisplayName AS dname, Score FROM Users.Users U
JOIN Posts.Posts P ON U.id = P.OwnerUserId
WHERE AcceptedAnswerId IS NOT NULL 
AND Score IN (SELECT Score FROM posts.posts
              WHERE AcceptedAnswerId IS NOT NULL 
              GROUP BY Score
              HAVING Score < 0) 
AND Tags LIKE '%|postgresql|%'
ORDER BY Score
LIMIT 5;

EXPLAIN ANALYZE SELECT DisplayName AS dname, Score FROM Users.Users U
JOIN Posts.Posts P ON U.id = P.OwnerUserId
WHERE AcceptedAnswerId IS NOT NULL 
AND Score IN (SELECT Score FROM posts.posts
              WHERE AcceptedAnswerId IS NOT NULL 
              GROUP BY Score
              HAVING Score < 0) 
AND Tags LIKE '%|postgresql|%'
ORDER BY Score
LIMIT 5;