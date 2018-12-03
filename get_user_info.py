from slackclient import SlackClient
from pprint import pprint
import os
import pickle


def init():
    # remember to set the token as one of the environment parameters
    slack_token = os.environ["SLACK_API_TOKEN"]
    global sc
    sc = SlackClient(slack_token)


def access_cache(name, update, func):
    if update:
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
        raw_channels = sc.api_call("channels.list")["channels"]
        channels = {}
        for c in raw_channels:
            channels[c['name']] = c['id']
        return channels

    return access_cache("channels", update, func)


def get_channel_history(channel_id, update=False):
    def func():
        raw_history = sc.api_call(
            "channels.history",
            channel=channel_id,
            count=1000
        )
        return raw_history['messages']

    return access_cache("channel_"+channel_id+"_history", update, func)


def get_all_history(update=False):
    def func():
        channels = get_channels(update=update)
        all_messages = []
        for c in channels.values():
            all_messages.append(get_channel_history(c))

        return [message for channel in all_messages for message in channel]

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


def get_users_activity(messages, update=False):
    user_list = get_all_users(update=update)
    users = {}
    for m in messages:
        user = user_list[m['user']]
        if user in users.keys():
            users[user] += 1
        else:
            users[user] = 1
    return [(key, users[key]) for key in sorted(users, key=users.get, reverse=True)]

init()
chans = get_channels(update=False)
print("\nList of channels searched: ")
print(list(chans.keys()))
print("\nNumber of total users:", len(get_all_users(update=False)))
print("\nNumber of total messages in all public channels:", len(get_all_history(update=False)))
top = 20
print("\nNumber of messages sent by the top {} users:  ".format(top))
pprint(get_users_activity(get_all_history())[:top])

