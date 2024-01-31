This is a proof-of-concept implementation of a readonly WebDAV interface to
DANDI Archive.

> [!NOTE]
> This project has been abandoned in favor of a Rust implementation at
> <https://github.com/dandi/dandidav>.

Installation
============
`dandidav` requires Python 3.9 or higher.  Just use [pip](https://pip.pypa.io)
for Python 3 (You have pip, right?) to install it:

    python3 -m pip install git+https://github.com/dandi/dandi-webdav


Usage
=====

- Run the `dandidav` command; the WebDAV server will be accessible for as long
  as the program is left running

- Visit http://127.0.0.1:8080 in any WebDAV client (including a regular web
  browser)

    - If your client asks you about login details, you may log in without
      authentication/as a guest.

- Shut down the server by hitting Ctrl-C
