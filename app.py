from cold_reader import *  # noqa: F401,F403
from cold_reader import main  # noqa: F401

# WSGI/ASGI-style discovery name is not applicable to the stdlib HTTP server,
# but expose the server factory for tooling that imports ``app``.
def create_server(*args, **kwargs):
    from coldarchive.server import ColdReaderHttpServer
    return ColdReaderHttpServer(*args, **kwargs)
