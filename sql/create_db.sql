\c postgres
DROP IF EXISTS DATABASE pg1;
CREATE DATABASE pg1;

\c pg1

CREATE SCHEMA IF NOT EXISTS Users;
CREATE SCHEMA IF NOT EXISTS Posts;
CREATE SCHEMA IF NOT EXISTS Tags;
CREATE SCHEMA IF NOT EXISTS History;
CREATE SCHEMA IF NOT EXISTS Votes;

-- замена типов данных: TYNIINT->SMALLINT, DATETIME->TIMESTAMP, NVARCHAR->TEXT, BYTE -> BOOLEAN (удобнее взаимодействовать с pythonl)

CREATE TABLE IF NOT EXISTS Users.Users( 
    Id INTEGER PRIMARY KEY,
    Reputation INTEGER, 
    CreationDate TIMESTAMP,
    DisplayName TEXT,
    LastAccessDate TIMESTAMP,
    WebsiteUrl TEXT,
    Location TEXT, 
    AboutMe TEXT, 
    Views INTEGER, 
    UpVotes INTEGER,
    DownVotes INTEGER,
    AccountId INTEGER
);

CREATE TABLE IF NOT EXISTS Posts.Posts(
    Id INTEGER PRIMARY KEY,
    PostTypeId SMALLINT, -- в схеме есть внешний ключ на таблицу, которой нет в файле, поэтому она не пишется здесь
    AcceptedAnswerId INTEGER, --REFERENCES Posts(Id)
    ParentId INTEGER,
    CreationDate TIMESTAMP, 
    Score INTEGER, 
    ViewCount INTEGER,
    Body TEXT,
    OwnerUserId INTEGER, --REFERENCES Users.Users(Id)
    OwnerDisplayName TEXT,
    LastEditorUserId INTEGER, --REFERENCES Users.Users(Id)
    LastEditorDisplayName TEXT,
    LastEditDate TIMESTAMP, 
    LastActivityDate TIMESTAMP,
    Title TEXT, 
    Tags TEXT, 
    AnswerCount INTEGER, 
    CommentCount INTEGER, 
    FavoriteCount INTEGER,
    ClosedDate TIMESTAMP,
    CommunityOwnedDate TIMESTAMP,
    ContentLicense TEXT -- в схеме такого поля нет, оно было найдено в xml-файле благодаря алгоритму поиска,
                        -- описанному выше
);

CREATE TABLE IF NOT EXISTS Users.Badges (
    Id INTEGER PRIMARY KEY,
    UserId INTEGER, --REFERENCES Users.Users(id)
    Name TEXT,  
    Date TIMESTAMP,
    Class SMALLINT,
    TagBased BOOLEAN
);

CREATE TABLE IF NOT EXISTS Posts.Comments(
    Id INTEGER PRIMARY KEY,
    PostId INTEGER, --REFERENCES Posts.Posts(Id)
    Score INTEGER, 
    Text TEXT, 
    CreationDate TIMESTAMP, 
    UserDisplayName TEXT,
    UserId INTEGER
);

CREATE TABLE IF NOT EXISTS History.PostHistory (
    Id INTEGER PRIMARY KEY,
    PostHistoryTypeId SMALLINT, -- в схеме есть внешний ключ на таблицу, которой нет в файле, поэтому она не пишется здесь
    PostId INTEGER, --REFERENCES Posts.Posts(Id)
    RevisionGUID TEXT, 
    CreationDate TIMESTAMP,
    UserId INTEGER, --REFERENCES Users.Users(Id)
    UserDisplayName TEXT,
    Comment TEXT,
    Text TEXT, 
    ContentLicense TEXT -- в базе данных такого поля нет, оно тоже было найдено в xml
);

CREATE TABLE IF NOT EXISTS Posts.PostLinks (
    Id INTEGER PRIMARY KEY,
    CreationDate TIMESTAMP, 
    PostId INTEGER, --REFERENCES Posts.Posts(Id)
    RelatedPostId INTEGER, --REFERENCES Posts.Posts(Id)
    LinkTypeId SMALLINT
);

CREATE TABLE IF NOT EXISTS Tags.Tags(
    Id INTEGER PRIMARY KEY, -- в схеме есть внешний ключ на таблицу, которой нет в файле, поэтому она не пишется здесь
    TagName TEXT, 
    Count INTEGER, 
    ExcerptPostId INTEGER, --REFERENCES Posts.Posts(id) 
    WikiPostId INTEGER, --REFERENCES Posts.Posts(id)
    IsModeratorOnly BOOLEAN,-- новое поле обнаружено при поиске уникальных полей
    IsRequired BOOLEAN -- второе новое поле обнаружено при поиске уникальных полей
);

CREATE TABLE IF NOT EXISTS Votes.Votes(
    Id INTEGER PRIMARY KEY,
    PostId INTEGER, --REFERENCES Posts(id)
    VoteTypeId SMALLINT, -- в схеме есть внешний ключ на таблицу, которой нет в файле, поэтому она не пишется здесь
    CreationDate TIMESTAMP
);