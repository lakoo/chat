import skygear
from skygear.models import DirectAccessControlEntry, PublicAccessControlEntry
from skygear.utils.context import current_user_id
from skygear.action import push_user
from skygear.container import SkygearContainer
from .utils import MASTER_KEY
from .utils import _get_conversation

from .exc import SkygearChatException
from .pubsub import _publish_event
from .user_conversation import UserConversation

import logging
log = logging.getLogger()

@skygear.before_save("conversation", async=False)
def handle_conversation_before_save(record, original_record, conn):
    validate_conversation(record)
    if record.get('is_direct_message'):
        record['admin_ids'] = record['participant_ids']
    if len(record.get('admin_ids', [])) == 0:
        record['admin_ids'] = record['participant_ids']

    is_new = original_record is None
    # Check permission
    if not is_new:
        if current_user_id() not in original_record.get('admin_ids', []):
            raise SkygearChatException("no permission to edit conversation")
    else:
        if current_user_id() not in record['participant_ids']:
            raise SkygearChatException(
                "cannot create conversations for other users")

    # Set the correct ACL at server side
    record._acl = [PublicAccessControlEntry('read')]
    for admin_id in record['admin_ids']:
        if admin_id in record['participant_ids']:
            record.acl.append(DirectAccessControlEntry(admin_id, 'write'))


def validate_conversation(record):
    if len(record.get('participant_ids', [])) == 0:
        raise SkygearChatException("converation must have participant")
    if record.get('is_direct_message'):
        if len(record['participant_ids']) != 2:
            raise SkygearChatException(
                "direct message must only have two participants")
    if not set(record['participant_ids']) >= set(record['admin_ids']):
        raise SkygearChatException(
            "admins should also be participants")

    for user_id in record.get('participant_ids', []):
        validate_user_id(user_id)
    for user_id in record.get('admin_ids', []):
        validate_user_id(user_id)


def validate_user_id(user_id):
    if user_id.startswith('user/'):
        raise SkygearChatException("user_id is not in correct format")


@skygear.after_save("conversation", async=False)
def handle_conversation_after_save(record, original_record, conn):
    if original_record is None:
        orig_participant = set()
    else:
        orig_participant = set(original_record['participant_ids'])
    participant = set(record['participant_ids'])

    # Create and remove
    uc = UserConversation(record.id)
    to_create = participant - orig_participant
    uc.create(to_create)
    to_delete = orig_participant - participant
    uc.delete(to_delete)


@skygear.after_save("conversation")
def pubsub_conversation_after_save(record, original_record, conn):
    p_ids = set(record['participant_ids'])
    if original_record is not None:
        orig_participant = set(original_record['participant_ids'])
        p_ids = p_ids | orig_participant

    # Notification
    container = SkygearContainer(api_key=MASTER_KEY)
    log.debug('pubsub_conversation_after_save record[is_picked_up]: %s original_record[is_picked_up]: %s', record['is_picked_up'], original_record['is_picked_up'])
    log.debug('pubsub_conversation_after_save record[is_active]: %s original_record[is_active]: %s', record['is_active'], original_record['is_active'])
    if record['end_by'] is not None:
        log.debug('pubsub_conversation_after_save record[end_by]: %s', record['end_by'])
    for p_id in p_ids:
        _publish_event(
            p_id, "conversation", "update", record, original_record)
        if p_id == record.created_by:
            if original_record is not None:
                if record['is_picked_up'] == True and original_record['is_picked_up'] == False:
                    push_user(
                        container, p_id, {
                            'apns': {
                                'aps': {
                                    'alert': '已有輔導員開始線上輔導',
                                },
                                'from': 'skygear',
                                'operation': 'notification',
                            },
                            'gcm': {
                                'notification': {
                                    'title': '',
                                    'body': '已有輔導員開始線上輔導',
                                },
                                'data': {
                                    'from': 'skygear',
                                    'operation': 'notification',
                                },
                            }
                        })
                if record['is_active'] == False and original_record['is_active'] == True and record['end_by'] is not None:
                    if record['end_by'] != p_id:
                        push_user(
                            container, p_id, {
                                'apns': {
                                    'aps': {
                                        'alert': '線上輔導已結束',
                                    },
                                    'from': 'skygear',
                                    'operation': 'notification',
                                },
                                'gcm': {
                                    'notification': {
                                        'title': '',
                                        'body': '線上輔導已結束',
                                    },
                                    'data': {
                                        'from': 'skygear',
                                        'operation': 'notification',
                                    },
                                }
                            })



@skygear.before_delete("conversation", async=False)
def handle_conversation_before_delete(record, conn):
    if current_user_id() not in record['admin_ids']:
        raise SkygearChatException("no permission to delete conversation")


@skygear.after_delete("conversation")
def handle_conversation_after_delete(record, conn):
    for p_id in record['participant_ids']:
        _publish_event(
            p_id, "conversation", "delete", record)
