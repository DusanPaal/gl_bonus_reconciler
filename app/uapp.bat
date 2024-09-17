ECHO %*
env\Scripts\python.exe app.py --action "reconcile" --email_id %* --database "remote"