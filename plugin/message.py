import strict_rfc3339
from psycopg2.extensions import AsIs

import skygear
from skygear.utils import db
from skygear.action import push_user
from skygear.utils.context import current_user_id
from skygear.container import SkygearContainer

from .asset import sign_asset_url
from .exc import SkygearChatException
from .pubsub import _publish_event, publish_message_in_crm_channel
from .utils import _get_conversation, schema_name
from .utils import MASTER_KEY
import logging
log = logging.getLogger()


@skygear.before_save("message", async=False)
def handle_message_before_save(record, original_record, conn):
    conversation = _get_conversation(record['conversation_id'])

    if current_user_id() not in conversation.get('participant_ids', []):
        raise SkygearChatException(
            "user not in conversation, permission denied")

    if original_record is not None:
        raise SkygearChatException("message is not editable")


@skygear.after_save("message")
def handle_message_after_save(record, original_record, conn):
    publish_message_in_crm_channel(record)
    conversation = _get_conversation(record['conversation_id'])
    container = SkygearContainer(api_key=MASTER_KEY)
    for p_id in conversation['participant_ids']:
        _publish_event(
            p_id, "message", "create", record)
        log.debug('handle_message_after_save ' + record + 'conversation created by: '+ conversation)
        push_user(
            container, p_id, {
                'apns': {
                    'aps': {
                        'alert': record['body'],
                    },
                    'from': 'skygear',
                    'operation': 'notification',
                },
                'gcm': {
                    'notification': {
                        'title': '',
                        'body': record['body'],
                    },
                    'data': {
                        'from': 'skygear',
                        'operation': 'notification',
                    },
                }
            })

    # Update all UserConversation unread count by 1
    conversation_id = record['conversation_id'].recordID.key

    conn.execute('''
        UPDATE %(schema_name)s.user_conversation
        SET "unread_count" = "unread_count" + 1
        WHERE "conversation" = %(conversation_id)s
    ''', {
        'schema_name': AsIs(schema_name),
        'conversation_id': conversation_id
    })
    conn.execute('''
        UPDATE %(schema_name)s.user_conversation
        SET "unread_count" = 0
        WHERE "conversation" = %(conversation_id)s
        AND "user" = %(user_id)s
    ''', {
        'schema_name': AsIs(schema_name),
        'conversation_id': conversation_id,
        'user_id': record.created_by
    })


@skygear.op("chat:get_messages", auth_required=True, user_required=True)
def get_messages(conversation_id, limit, before_time=None):
    conversation = _get_conversation(conversation_id)

    # TODO: Should verify the user is CRM user
    # if current_user_id() not in conversation['participant_ids']:
    #     raise SkygearChatException("user not in conversation")

    # FIXME: After the ACL can be by-pass the ACL, we should query the with
    # master key
    # https://github.com/SkygearIO/skygear-server/issues/51
    with db.conn() as conn:
        cur = conn.execute('''
            SELECT
                _id, _created_at, _created_by,
                body, conversation_id, metadata, attachment
            FROM %(schema_name)s.message
            WHERE conversation_id = %(conversation_id)s
            AND (_created_at < %(before_time)s OR %(before_time)s IS NULL)
            ORDER BY _created_at DESC
            LIMIT %(limit)s;
            ''', {
            'schema_name': AsIs(schema_name),
            'conversation_id': conversation_id,
            'before_time': before_time,
            'limit': limit
        }
        )

        results = []
        for row in cur:
            created_stamp = row[1].timestamp()
            dt = strict_rfc3339.timestamp_to_rfc3339_utcoffset(created_stamp)
            r = {
                '_id': 'message/' + row[0],
                '_created_at': dt,
                '_created_by': row[2],
                'body': row[3],
                'conversation_id': {
                    '$id': 'conversation/' + row[4],
                    '$type': 'ref'
                },
                'metadata': row[5],
            }
            if row[6]:
                r['attachment'] = {
                    '$type': 'asset',
                    '$name': row[6],
                    '$url': sign_asset_url(row[6])
                }
            results.append(r)
        resultsReverse=results[::-1]
        return {'results': resultsReverse}
