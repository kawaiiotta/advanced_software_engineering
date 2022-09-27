## Load packages

```
pip install  --upgrade pip
pip instal -r requirements.txt
```

in case you install additional packages.... update the requirements.txt with:

```
pip freeze > requirements.txt


```

## Running the discord bot
If you want to run bot which already exists in the iD8 labs discord

```
python bot
```

If you want to add bot to another server
In the ".env" file change the variable 'DISCORD_GUILD' to the name of the discord server of your choice