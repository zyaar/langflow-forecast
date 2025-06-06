from langflow.base.forecasting_common.context.forecast_shared_context import ForecastSharedContext

user_id_1 = "123"
user_id_2 = "456"
form_id_1 = "ABC"
form_id_2 = "DEF"

# check creating a singleton WITHOUT a user_id or flow_id
print(f"\n\nCreate singleton with NO user_id or flow_id")

singleton = ForecastSharedContext()
print(f"singleton = {singleton}, type={type(singleton)}")

if(singleton is None):
    print(f"No singleton returned")
else:
    print(f"Singleton returned")


# check creating a singleton WITH ONLY user_id
print(f"\n\nCreate singleton with user_id={user_id_1}")

singleton = ForecastSharedContext(user_id = user_id_1)

if(singleton is None):
    print(f"No singleton returned")
else:
    print(f"Singleton returned")

print(f"singleton = {singleton}, type={type(singleton)}")

new_singleton = ForecastSharedContext(user_id = user_id_1)

if(new_singleton is None):
    print(f"No singleton returned")
else:
    print(f"Singleton returned")

print(f"new_singleton = {new_singleton}, type={type(new_singleton)}")
print(f"singleton is new_singleton? {singleton is new_singleton}")



print(f"\n\nCreate singleton with user_id={user_id_2}")

newer_singleton = ForecastSharedContext(user_id = user_id_2)

if(newer_singleton is None):
    print(f"No singleton returned")
else:
    print(f"Singleton returned")

print(f"newer_singleton = {newer_singleton}, type={type(newer_singleton)}")
print(f"newer_singleton is singleton? {newer_singleton is singleton}")
print(f"newer_singleton is new_singleton? {newer_singleton is new_singleton}")

newest_singleton = ForecastSharedContext(user_id = user_id_2)

if(newest_singleton is None):
    print(f"No singleton returned")
else:
    print(f"Singleton returned")

print(f"newest_singleton = {newest_singleton}, type={type(newest_singleton)}")
print(f"newest_singleton is singleton? {newest_singleton is singleton}")
print(f"newest_singleton is new_singleton? {newest_singleton is new_singleton}")
print(f"newest_singleton is newer_singleton? {newest_singleton is newer_singleton}")


# check if we can add an element to one singleton and it shows up in the appropriate areas
print(f"\n\nsingleton.attr['xxx'] = 'yyy'")
singleton.attr["xxx"] = "yyy"

print(f"singleton.attr = {singleton.attr}")
print(f"new_singleton.attr = {new_singleton.attr}")
print(f"newer_singleton.attr = {newer_singleton.attr}")
print(f"newest_singleton.attr = {newest_singleton.attr}")

print(f"\n\nnewest_singleton.attr['ABC'] = 123")
newest_singleton.attr["ABC"] = 123

print(f"singleton.attr = {singleton.attr}")
print(f"new_singleton.attr = {new_singleton.attr}")
print(f"newer_singleton.attr = {newer_singleton.attr}")
print(f"newest_singleton.attr = {newest_singleton.attr}")

