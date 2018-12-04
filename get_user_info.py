from slackclient import SlackClient
from pprint import pprint
import os
import pickle


def init():
    # remember to set the token as one of the environment parameters
    # https://slackapi.github.io/python-slackclient/auth.html#handling-tokens-and-other-sensitive-data
    slack_token = os.environ["SLACK_API_TOKEN"]
    global sc
    sc = SlackClient(slack_token)


def access_cache(name, update, func, force_update=False):
    if update or force_update:
        data = func()
        pickle.dump(data, open('cache/'+str(name)+".pkl", 'wb'))
        return data
    try:
        return pickle.load(open('cache/'+str(name)+".pkl", 'rb'))
    except FileNotFoundError:
        # force update
        return access_cache(name, True, func)


def get_channels(update=False):
    def func():
        raw_channels = sc.api_call("conversations.list",
                                   types="public_channel, private_channel, mpim, im")["channels"]
        channels = {}
        for c in raw_channels:
            if 'name' in c:
                channels[c['name']] = c['id']
            else:
                channels[c['id']] = c['id']
        return channels

    return access_cache("channels", update, func)

def get_conversation_history(channel_id, update=False):
    def func():
        raw_history = sc.api_call(
            "conversations.history",
            channel=channel_id,
            count=1000
        )
        for msg in raw_history['messages']:
            if 'replies' in msg:
                for reply in msg['replies']:
                    reply['text'] = "N/A; was found in thread"
                    raw_history['messages'].append(reply)

        return raw_history['messages']

    return access_cache("conversation_"+channel_id+"_history", update, func)


def get_all_history(update=False, return_channel_breakdown=False):
    def func():
        channels = get_channels(update=update)
        all_messages = []
        channel_breakdown = {}
        for c in channels.values():
            h = get_conversation_history(c)
            channel_breakdown[c] = h
            all_messages.append(h)

        # flatten
        history = [message for channel in all_messages for message in channel]
        if return_channel_breakdown:
            return history, channel_breakdown
        else:
            return history

    return access_cache("all_history", update, func)


def get_all_users(update=False):
    def func():
        raw_users = sc.api_call(
            "users.list",
            count=1000
        )
        users = {}
        for mem in raw_users['members']:
            users[mem['id']] = mem['real_name']
        return users

    return access_cache("userlist", update, func)


def get_top_users(messages, update=False):
    user_list = get_all_users(update=update)
    users = {}
    for m in messages:
        user = user_list[m['user']]
        if user in users.keys():
            users[user] += 1
        else:
            users[user] = 1
    return [(key, users[key]) for key in sorted(users, key=users.get, reverse=True)]


def get_user_activity(name, update=False):
    def func():
        user_list = {kv[1]: kv[0] for kv in get_all_users().items()}
        user_id = user_list[name]
        chan_list = {kv[1]: kv[0] for kv in get_channels().items()}

        history, channel_history = get_all_history(return_channel_breakdown=True, update=update)

        channels = {}

        for chn in channel_history.items():
            channel_name = chan_list[chn[0]]
            channels[channel_name] = []
            for msg in chn[1]:
                if msg['user'] == user_id:
                    channels[channel_name].append(msg['text'])
        return channels

    return access_cache("user_"+name, update, func)


init()
chans = get_channels()
users = get_all_users()
history, channel_history = get_all_history(return_channel_breakdown=True)

print("\nList of channels searched:", list(chans.keys()))
print("\nNumber of total users:", len(users))
print("\nNumber of total messages in all channels:", len(history))

top = 20
print("\nNumber of messages sent by the top {} users:  ".format(top))
user_activity = get_top_users(history)
pprint(user_activity[:top])

# user-specific
name = "Connor Richards"
print("\nMessages sent by", name)
for c in get_user_activity(name).items():
    if len(c[1]) > 0:
        print(c[0], ": ", len(c[1]))


