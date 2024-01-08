This is a proof-of-concept implementation of a readonly WebDAV interface to
DANDI Archive.

To use:

- Install the dependencies listed in `requirements.txt`.  Python 3.9+ is
  required.
- Run `python3 dandidav.py`; the WebDAV server will be accessible for as long
  as the script is left running
- Visit http://127.0.0.1:8080 in any WebDAV client (including a regular web
  browser)
    - If your client asks you about login details, you may log in without
      authentication/as a guest.
- Shut down the server by hitting Ctrl-C
