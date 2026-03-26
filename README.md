# TAI-project2


# Creating Project Service Accounts
There are a few ways to create a service account for your project. I am going to lead you through how to do it on the website.
- Log into google cloud console on the web
- Make sure the project listed between "Google Cloud" and the search bar in the app bar is the one you want to create a service account for
    - If it is not, then click on it and select your desired project or create one with "New project" in the top right of the pop-up
- Click on the three vertical dots in the top right corner, next to your profile picture
- Select the "Project settings" option
- Hover over any of the icons in the drawer on the right to read your options and select "Service Accounts"
- Click on "Create service account" to create a service account
- Fill out "Service account ID" and then click "Create and continue"
    - Here you can fill out any of the other fields to meet your needs and click "Done" once finished
- Click on the hyperlink in the email column for your service account
- Navigate to "Keys"
- Click on the "Add key" dropdown
    - Select "Create new key"
    - Select "JSON". Which will download a json file to your device
- MAKE SURE YOU STORE THE FILE IN A SECURE LOCATION AND DO NOT LET IT BE EXPOSED
- Add the path to the JSON key you downloaded to a .env file that is included in your .gitignore
- include this bit of code before you initialize any google cloud API client and make sure to replace <> with what you set the path to in your .env file: 
    - from dotenv import load_dotenv
      from os import getenv, environ

      load_dotenv()
      key_path = getenv("<whatever variable you stored the path in>")
      environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
