from fastapi import FastAPI, Depends
from sqlmodel import SQLModel, Session, select
from apps.database import engine, get_session
from apps.models.channel_mix import ChannelMix

app = FastAPI()


# Create tables at startup
