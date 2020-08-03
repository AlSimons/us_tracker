from sqlalchemy.ext.declarative import declarative_base

Base = None


def get_base():
    global Base
    if Base is None:
        Base = declarative_base()
    return Base
