from sqlalchemy import Column, Integer, Float, ForeignKey, String
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# The table for the reporting entities (countries, states, counties, cities)
# We'll call these "Location" for now, unless I come up with a better name.


class Location(Base):
    __tablename__ = 'location'

    id = Column(Integer, primary_key=True)
    jhu_key = Column(String(80), unique=True, nullable=False)
    country = Column(String(40), nullable=False)
    fips = Column(Integer)  # US Only: FIPS unique code for a location
    admin1 = Column(String(60))  # State / Province
    admin2 = Column(String(60))  # County / City
    # For now, we'll ignore all the other location information


# The table for daily stats
class Datum(Base):
    __tablename__ = 'datum'

    id = Column(Integer, primary_key=True)
    ordinal_date = Column(Integer, nullable=False)
    location_jhu_key = Column(String(80), ForeignKey('location.jhu_key'))
    confirmed = Column(Integer)
    deaths = Column(Integer)
    recovered = Column(Integer)
    active = Column(Integer)
    incidence_rate = Column(Float)
    case_fatality_ratio = Column(Float)
    location = relationship(Location, back_populates="data")


Location.data = relationship("Datum", order_by=Datum.ordinal_date,
                             back_populates="location")


class LastDate(Base):
    __tablename__ = 'last_date'

    id = Column(Integer, primary_key=True)
    ordinal_date = Column(Integer, nullable=False)
    date_string = Column(String(10))
