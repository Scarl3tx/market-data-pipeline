from threading import RLock

# Shared by all database loaders in this Python process.
DATABASE_WRITE_LOCK = RLock()