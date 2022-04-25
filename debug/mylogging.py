import logging, sys, os

def _generate_local_logger(env_level=None):
    import logging, sys, os

    if env_level == None:
        env_level = os.environ.get("PYTHONDEBUGLEVEL", 0)

    try:
        env_level = int(env_level)
    except:
        env_level = 0

    g_logger = logging.getLogger(__name__)
    _stream_handler = logging.StreamHandler(sys.stderr)
    _formatter = logging.Formatter(
        "\033[1m%(levelname)s\033[0m:%(name)s:%(lineno)d:%(message)s"
    )
    _stream_handler.setFormatter(_formatter)
    g_logger.addHandler(_stream_handler)
    g_logger.setLevel(env_level)

    return g_logger


g_logger = _generate_local_logger()

