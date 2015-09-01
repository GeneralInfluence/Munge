



# Takes in a group name and outputs a .json file that has the same name as the input parameter
# USe of this function will also require an api key
def WriteMeetupEvents(GroupName):
    #Put in your API key here
    #You can get one from https://secure.meetup.com/meetup_api/key/
    api_key = "5db37233e5711225b62251680323943"

    #Get request for the events list of a group
    #This is simplified using the python requests library
    response = requests.get("https://api.meetup.com/2/events?key=5db37233e5711225b62251680323943&group_urlname="+GroupName+"&sign=true")

    #Write the server respons as a json
    #This is unparsed.
    with open(GroupName+'.json', 'w') as fp:
        json.dump(response.json(), fp)


# Takes in a group name and outputs a .json file that has the same name as the input parameter
# USe of this function will also require an api key
def WriteMeetupEvents(GroupName):
    #Put in your API key here
    #You can get one from https://secure.meetup.com/meetup_api/key/
    api_key = "5db37233e5711225b62251680323943"

    #Get request for the events list of a group
    #This is simplified using the python requests library
    response = requests.get("https://api.meetup.com/2/events?key=5db37233e5711225b62251680323943&group_urlname="+GroupName+"&sign=true")

    #Write the server respons as a json
    #This is unparsed.
    with open(GroupName+'.json', 'w') as fp:
        json.dump(response.json(), fp)


# In[14]:

WriteMeetupEvents('Data-Community-DC')

