import xml.etree.ElementTree as ET
import psycopg2
from psycopg2 import Error
from psycopg2 import sql

from funcs import create_connection as crtcnt
from funcs import execute_query
from funcs import executemany_query
from funcs import get_all_coloumns_list
from funcs import check_all_coloumns_list
from funcs import add_key
from funcs import get_all_strings


if __name__ == '__main__':

    conn = crtcnt(
    'pg1', 'eugene', 'postgres', 'localhost', '5432'
    )

    if conn is not None:


        ##### Users.xml #####
        users_path = '/home/mephist/postgresql/scripts/xml/dba.stackexchange.com/Users.xml'

        users_coloumns_list = get_all_coloumns_list(users_path)
        users_check_coloumns_list = check_all_coloumns_list(users_path)
        users_string_list = get_all_strings(users_path, users_coloumns_list)

        if len(users_coloumns_list)==len(users_check_coloumns_list):
            print('automatic insert in table: Users.Users')
            users_fields_str = '('+(', ').join(users_coloumns_list)+')'
            users_format_str = '%s, '*len(users_coloumns_list) # чтобы вручную не прописывать много раз %s
            users_query = ('INSERT INTO {table} {fields} VALUES {form};').format(
                    fields = users_fields_str,
                    table='Users.Users',
                    form='('+users_format_str[:len(users_format_str)-2]+')' # удаление лишних символов из конца строки 
            )
            executemany_query(conn, users_query, users_string_list)
        else:
            pass # так как предыдущая проверка прошла, здесь проверка убрана в целях
                 # сокращения кода реализация блока else пропущена  


        ##### Posts.xml #####
        posts_path = '/home/mephist/postgresql/scripts/xml/dba.stackexchange.com/Posts.xml'

        posts_coloumns_list = get_all_coloumns_list(posts_path)
        posts_check_coloumns_list = check_all_coloumns_list(posts_path)

        print(posts_check_coloumns_list)

        if len(posts_coloumns_list)==len(posts_check_coloumns_list):
            pass # так как предыдущая проверка прошла, здесь проверка убрана в целях
                 # сокращения кода реализация блока else пропущена  
        else:
            print('you need in a handwork with a table: Posts.Posts')

            posts_coloumns_list_true = [
                'Id', 'PostTypeId', 'AcceptedAnswerId', 'ParentId', 'CreationDate',
                'Score', 'ViewCount', 'Body', 'OwnerUserId', 'OwnerDisplayName',
                'LastEditorUserId', 'LastEditorDisplayName', 'LastEditDate', 
                'LastActivityDate', 'Title', 'Tags', 'AnswerCount', 'CommentCount',
                'FavoriteCount', 'ClosedDate',  'CommunityOwnedDate', 'ContentLicense' 
            ] # в ходе поиска уникальных полей в xml-файле было найдено поле ContentLicense, 
              # которого нет в схеме данных. Напротив, поля DeletionDate нет в xml
              # положение поля ContentLicense в таблице Posts было найдено следующим образом:
              # всего в таблице Posts найдено 22 уникальных поля. По первой строке xml было 
              # определено 16 полей, находящихся перед полем DeletionDate. Положенеи ещё 3 полей удалось 
              # определить из схемы базы данных. Оставшиеся 2 - вручную, по файлу с помощью ctrl+f
              # В результате определено, что поле ContentLicense надо добавить в конец таблицы

            posts_string_list_true = get_all_strings(posts_path, posts_coloumns_list_true)
        
            posts_fields_str = '('+(', ').join(posts_coloumns_list_true)+')'
            posts_format_str = '%s, '*len(posts_coloumns_list_true)
            posts_query = ('INSERT INTO {table} {fields} VALUES {form};').format(
                    fields = posts_fields_str,
                    table='Posts.Posts',
                    form='('+posts_format_str[:len(posts_format_str)-2]+')' 
            )
            executemany_query(conn, posts_query, posts_string_list_true)

        posts_insert_query = '''
            INSERT INTO Posts.Posts (id) 
            SELECT DISTINCT AcceptedAnswerId FROM Posts.Posts
            WHERE AcceptedAnswerId NOT IN (SELECT id FROM Posts.Posts); 
        ''' # в поле AcceptedAnswerId таблицы Posts есть записи со значениями,
            # которых нет в поле id таблицы Posts, но при этом в поле 
            # AcceptedAnswerId есть ограничение в виде внешнего ключа на Posts.id
            # Во избежание потерь данных в Posts.id добавлены уникальные 
            # из Posts.AcceptedAnswerId, которых нет в Posts.id
        execute_query(conn, posts_insert_query)

        posts_alter_query = '''
            ALTER TABLE Posts.Posts
            ADD CONSTRAINT fk_AcceptedAnswerId 
            FOREIGN KEY(AcceptedAnswerId) REFERENCES Posts.Posts(id);
        ''' # Поле AcceptedAnswerId ссылается на поле id этой же таблицы.
            # Если прописать сразу внешний ключ, будет ошибка, так как 
            # таблица Posts еще не содержит данные и внешний ключ ссылается вникуда
            # Поэтому сначала надо заполнить таблицу Posts данными, а потом через
            # ALTER обновить поле AcceptedAnswerId, добавив ограничение в виде
            # внешнего ключа

        execute_query(conn, posts_alter_query)

        users_insert_query = '''
            INSERT INTO Users.Users (id) 
            SELECT DISTINCT OwnerUserId, LastEditorUserId FROM Posts.Posts
            WHERE OwnerUserId NOT IN (SELECT id FROM Users.Users) AND LastEditorUserId NOT IN (SELECT id FROM Users.Users); 
        ''' # в полях LastEditorUserId, OwnerUserId таблицы Posts есть записи со значениями,
            # которых нет в поле id таблицы Users, но при этом в поле 
            # LastEditorUserId, OwnerUserId есть ограничение в виде внешнего ключа на Users.id
            # Во избежание потерь данных в Users.id добавлены уникальные 
            # из Posts.OwnerUserId, которых нет в Users.id
        execute_query(conn, users_insert_query)

        users_alter_query = '''
            ALTER TABLE Posts.Posts
            ADD CONSTRAINT fk_OwnerUserId
            FOREIGN KEY(OwnerUserId) REFERENCES Users.Users(id),
            ADD CONSTRAINT fk_LastEditorUserId
            FOREIGN KEY(LastEditorUserId) REFERENCES Users.Users(id);
        ''' # Теперь после добвления данных в Users.id можно
            # можно добавить внешний ключ
        execute_query(conn, users_alter_query)


        ##### Badges.xml #####
        badges_path = '/home/mephist/postgresql/scripts/xml/Badges.xml'

        badges_coloumns_list = get_all_coloumns_list(badges_path)
        badges_check_coloumns_list = check_all_coloumns_list(badges_path)
        badges_string_list = get_all_strings(badges_path, badges_coloumns_list)

        if len(badges_coloumns_list)==len(badges_check_coloumns_list):
            print('automatic insert in table: Users.Badges')
            badges_fields_str = '('+(', ').join(badges_coloumns_list)+')'
            badges_format_str = '%s, '*len(badges_coloumns_list)
            badges_query = ('INSERT INTO {table} {fields} VALUES {form};').format(
                    fields = badges_fields_str,
                    table='Users.Badges',
                    form='('+badges_format_str[:len(badges_format_str)-2]+')'
            )
            executemany_query(conn, badges_query, badges_string_list)
        else:
            pass # так как предыдущая проверка прошла, здесь проверка убрана в целях
                 # сокращения кода реализация блока else пропущена   

        badges_insert_query = '''
            INSERT INTO Users.Users (id) 
            SELECT DISTINCT UserId FROM Users.Badges
            WHERE UserId NOT IN (SELECT id FROM Users.Users); 
        ''' # Комментарий аналогичен предыдущим
        execute_query(conn, badges_insert_query)

        badges_alter_query = '''
            ALTER TABLE Users.Badges
            ADD CONSTRAINT fk_UserId
            FOREIGN KEY(UserId) REFERENCES Users.Users(id);
        ''' # Комментарий аналогичен предыдущим
        execute_query(conn, badges_alter_query)


        ##### Comments.xml #####
        comments_path = '/home/mephist/postgresql/scripts/xml/Comments.xml'

        comments_coloumns_list = get_all_coloumns_list(comments_path)
        comments_check_coloumns_list = check_all_coloumns_list(comments_path)
        comments_string_list = get_all_strings(comments_path, comments_coloumns_list)

        if len(comments_coloumns_list)==len(comments_check_coloumns_list):
            pass # так как проверка не прошла, она убрана в целях
                 # сокращения кода. Реализация блока else представлена ниже
        else:
            print('you need in a handwork with a table: Posts.Comments')

            comments_coloumns_list_true = [
                'Id', 'PostId', 'Score', 'Text', 'CreationDate', 
                'UserDisplayName', 'UserId'
            ] 

            comments_string_list_true = get_all_strings(comments_path, comments_coloumns_list_true)
        
            comments_fields_str = '('+(', ').join(comments_coloumns_list_true)+')'
            comments_format_str = '%s, '*len(comments_coloumns_list_true)
            comments_query = ('INSERT INTO {table} {fields} VALUES {form};').format(
                    fields = comments_fields_str,
                    table='Posts.Comments',
                    form='('+comments_format_str[:len(comments_format_str)-2]+')' 
            )
            executemany_query(conn, comments_query, comments_string_list_true)

        comments_insert_query = '''
            INSERT INTO Posts.Posts (id) 
            SELECT DISTINCT PostId FROM Posts.Comments
            WHERE PostId NOT IN (SELECT id FROM Posts.Posts); 
        ''' # Комментарий аналогичен предыдущим
        execute_query(conn, comments_insert_query)

        badges_alter_query = '''
            ALTER TABLE Posts.Comments
            ADD CONSTRAINT fk_PostId
            FOREIGN KEY(PostId) REFERENCES Posts.Posts(id);
        ''' # Комментарий аналогичен предыдущим
        execute_query(conn, badges_alter_query)


        ##### PostHistory.xml #####
        post_history_path = '/home/mephist/postgresql/scripts/xml/PostHistory.xml'

        post_history_coloumns_list = get_all_coloumns_list(post_history_path)
        post_history_check_coloumns_list = check_all_coloumns_list(post_history_path)
        post_history_string_list = get_all_strings(post_history_path, post_history_coloumns_list)

        if len(post_history_coloumns_list)==len(post_history_check_coloumns_list):
            pass
        else:
            print('you need in a handwork with a table: History.PostHistory')

            post_history_coloumns_list_true = [
                'Id', 'PostHistoryTypeId', 'PostId', 'RevisionGUID', 'CreationDate', 
                'UserId', 'UserDisplayName', 'Comment', 'Text', 'ContentLicense'
            ] 

            post_history_string_list_true = get_all_strings(post_history_path, post_history_coloumns_list_true)
        
            post_history_fields_str = '('+(', ').join(post_history_coloumns_list_true)+')'
            post_history_format_str = '%s, '*len(post_history_coloumns_list_true)
            post_history_query = ('INSERT INTO {table} {fields} VALUES {form};').format(
                    fields = post_history_fields_str,
                    table='History.PostHistory',
                    form='('+post_history_format_str[:len(post_history_format_str)-2]+')' 
            )
            executemany_query(conn, post_history_query, post_history_string_list_true)

        post_history_insert_query = '''
            INSERT INTO Posts.Posts (id) 
            SELECT DISTINCT PostId FROM History.PostHistory
            WHERE PostId NOT IN (SELECT id FROM Posts.Posts); 
        ''' # Комментарий аналогичен предыдущим
        execute_query(conn, post_history_insert_query)

        post_history_alter_query = '''
            ALTER TABLE History.PostHistory
            ADD CONSTRAINT fk_PostId
            FOREIGN KEY(PostId) REFERENCES Posts.Posts(id);
        ''' # Комментарий аналогичен предыдущим
        execute_query(conn, post_history_alter_query)

        post_history_insert_query_2 = '''
            INSERT INTO Users.Users (id) 
            SELECT DISTINCT UserId FROM History.PostHistory
            WHERE UserId NOT IN (SELECT id FROM Users.Users) AND UserId IS NOT NULL; 
        ''' # Комментарий аналогичен предыдущим
        execute_query(conn, post_history_insert_query_2)

        post_history_alter_query_2 = '''
            ALTER TABLE History.PostHistory
            ADD CONSTRAINT fk_UserId
            FOREIGN KEY(UserId) REFERENCES Users.Users(id);
        ''' # Комментарий аналогичен предыдущим
        execute_query(conn, post_history_alter_query_2)


        ##### PostLinks.xml #####
        post_links_path = '/home/mephist/postgresql/scripts/xml/PostLinks.xml'

        post_links_coloumns_list = get_all_coloumns_list(post_links_path)
        post_links_check_coloumns_list = check_all_coloumns_list(post_links_path)
        post_links_string_list = get_all_strings(post_links_path, post_links_coloumns_list)

        if len(post_links_coloumns_list)==len(post_links_check_coloumns_list):
            print('automatic insert in table: Posts.PostLinks')
            post_links_fields_str = '('+(', ').join(post_links_coloumns_list)+')'
            post_links_format_str = '%s, '*len(post_links_coloumns_list)
            post_links_query = ('INSERT INTO {table} {fields} VALUES {form};').format(
                    fields = post_links_fields_str,
                    table='Posts.PostLinks',
                    form='('+post_links_format_str[:len(post_links_format_str)-2]+')'
            )
            executemany_query(conn, post_links_query, post_links_string_list)
        else:
            pass

        post_links_insert_query = '''
            INSERT INTO Posts.Posts (Id)
            SELECT DISTINCT PostId, RelatedPostId FROM Posts.PostLinks
            WHERE PostId NOT IN (SELECT Id FROM Posts.Posts) AND RelatedPostId NOT IN (SELECT Id FROM Posts.Posts)
        '''
        post_links_alter_query = '''
            ALTER TABLE Posts.PostLinks
            ADD CONSTRAINT fk_PostId
            FOREIGN KEY(PostId) REFERENCES Posts.Posts(Id),
            ADD CONSTRAINT fk_RelatedPostId
            FOREIGN KEY(RelatedPostId) REFERENCES Posts.Posts(Id);
        '''
        execute_query(conn, post_links_alter_query)


        ##### Tags.xml #####
        tags_path = '/home/mephist/postgresql/scripts/xml/Tags.xml'

        tags_coloumns_list = get_all_coloumns_list(tags_path)
        tags_check_coloumns_list = check_all_coloumns_list(tags_path)
        tags_string_list = get_all_strings(tags_path, tags_coloumns_list)

        if len(tags_coloumns_list)==len(tags_check_coloumns_list):
            pass
        else:
            print('you need in a handwork with a table: Tags.Tags')

            tags_coloumns_list_true = [
                'Id', 'TagName', 'Count', 'ExcerptPostId', 
                'WikiPostId', 'IsModeratorOnly', 'IsRequired' 
            ] 

            tags_string_list_true = get_all_strings(tags_path, tags_coloumns_list_true)
        
            tags_fields_str = '('+(', ').join(tags_coloumns_list_true)+')'
            tags_format_str = '%s, '*len(tags_coloumns_list_true)
            tags_query = ('INSERT INTO {table} {fields} VALUES {form};').format(
                    fields = tags_fields_str,
                    table='Tags.Tags',
                    form='('+tags_format_str[:len(tags_format_str)-2]+')' 
            )
            executemany_query(conn, tags_query, tags_string_list_true)

        tags_insert_query = '''
            INSERT INTO Posts.Posts (Id)
            SELECT DISTINCT ExcerptPostId, WikiPostId FROM Tags.Tags
            WHERE ExcerptPostId NOT IN (SELECT Id FROM Posts.Posts) AND WikiPostId NOT IN (SELECT Id FROM Posts.Posts)
        '''
        tags_alter_query = '''
            ALTER TABLE tags.tags
            ADD CONSTRAINT fk_ExcerptPostId
            FOREIGN KEY(ExcerptPostId) REFERENCES Posts.Posts(Id), 
            ADD CONSTRAINT fk_WikiPostId
            FOREIGN KEY(WikiPostId) REFERENCES Posts.Posts(Id);
        '''
        execute_query(conn, tags_alter_query)


        ##### Votes.xml #####
        votes_path = '/home/mephist/postgresql/scripts/xml/Votes.xml'

        votes_coloumns_list = get_all_coloumns_list(votes_path)
        votes_check_coloumns_list = check_all_coloumns_list(votes_path)
        votes_string_list = get_all_strings(votes_path, votes_coloumns_list)

        if len(votes_coloumns_list)==len(votes_check_coloumns_list):
            print('automatic insert in table: Votes.Votes')
            votes_fields_str = '('+(', ').join(votes_coloumns_list)+')'
            votes_format_str = '%s, '*len(votes_coloumns_list)
            votes_query = ('INSERT INTO {table} {fields} VALUES {form};').format(
                    fields = votes_fields_str,
                    table='Votes.Votes',
                    form='('+votes_format_str[:len(votes_format_str)-2]+')'
            )
            executemany_query(conn, votes_query, votes_string_list)
        else:
            pass

        votes_insert_query = '''
            INSERT INTO Posts.Posts (id) 
            SELECT DISTINCT PostId FROM Votes.Votes
            WHERE PostId NOT IN (SELECT id FROM Posts.Posts); 
        '''
        execute_query(conn, votes_insert_query)

        votes_alter_query = '''
            ALTER TABLE Votes.Votes
            ADD CONSTRAINT fk_PostId 
            FOREIGN KEY(PostId) REFERENCES Posts.Posts(id);
        ''' 
        execute_query(conn, votes_alter_query)
    
    else:
        print('Error in connection!')
        
    conn.close()