"""SQLAlchemy ORM models for the logistics database."""
import uuid
from datetime import datetime, date
from typing import Optional

from sqlalchemy import (
    Column, String, Float, Date, DateTime, Boolean, Text,
    ForeignKey, JSON, Integer, UniqueConstraint, func
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


def _uuid() -> str:
    return str(uuid.uuid4())


class Carrier(Base):
    __tablename__ = "carriers"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    carrier_code = Column(String, nullable=False, unique=True)
    gstin = Column(String)
    bank_account = Column(String)
    status = Column(String, default="active")
    onboarded_on = Column(Date)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    contracts = relationship("CarrierContract", back_populates="carrier")
    shipments = relationship("Shipment", back_populates="carrier")
    freight_bills = relationship("FreightBill", back_populates="carrier")


class CarrierContract(Base):
    __tablename__ = "carrier_contracts"

    id = Column(String, primary_key=True)
    carrier_id = Column(String, ForeignKey("carriers.id"), nullable=False)
    effective_date = Column(Date, nullable=False)
    expiry_date = Column(Date, nullable=False)
    status = Column(String, default="active")
    notes = Column(Text)
    rate_card = Column(JSON, nullable=False)  # List of rate entries
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    carrier = relationship("Carrier", back_populates="contracts")
    shipments = relationship("Shipment", back_populates="contract")


class Shipment(Base):
    __tablename__ = "shipments"

    id = Column(String, primary_key=True)
    carrier_id = Column(String, ForeignKey("carriers.id"), nullable=False)
    contract_id = Column(String, ForeignKey("carrier_contracts.id"), nullable=False)
    lane = Column(String, nullable=False)
    shipment_date = Column(Date, nullable=False)
    status = Column(String, default="in_transit")
    total_weight_kg = Column(Float)
    notes = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    carrier = relationship("Carrier", back_populates="shipments")
    contract = relationship("CarrierContract", back_populates="shipments")
    bols = relationship("BillOfLading", back_populates="shipment")
    freight_bills = relationship("FreightBill", back_populates="shipment")


class BillOfLading(Base):
    __tablename__ = "bills_of_lading"

    id = Column(String, primary_key=True)
    shipment_id = Column(String, ForeignKey("shipments.id"), nullable=False)
    delivery_date = Column(Date)
    actual_weight_kg = Column(Float)
    notes = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    shipment = relationship("Shipment", back_populates="bols")


class FreightBill(Base):
    __tablename__ = "freight_bills"

    id = Column(String, primary_key=True)
    carrier_id = Column(String, ForeignKey("carriers.id"), nullable=True)
    carrier_name = Column(String, nullable=False)
    bill_number = Column(String, nullable=False)
    bill_date = Column(Date, nullable=False)
    shipment_reference = Column(String, ForeignKey("shipments.id"), nullable=True)
    lane = Column(String, nullable=False)
    billed_weight_kg = Column(Float, nullable=False)
    rate_per_kg = Column(Float)
    billing_unit = Column(String, default="kg")
    base_charge = Column(Float, nullable=False)
    fuel_surcharge = Column(Float, nullable=False)
    gst_amount = Column(Float, nullable=False)
    total_amount = Column(Float, nullable=False)

    # Agent-managed state
    status = Column(String, default="pending")
    # pending | processing | pending_review | auto_approved | disputed | approved | rejected | duplicate
    langgraph_thread_id = Column(String, unique=True)
    raw_payload = Column(JSON)  # original payload stored for audit

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    carrier = relationship("Carrier", back_populates="freight_bills", foreign_keys=[carrier_id])
    shipment = relationship("Shipment", back_populates="freight_bills", foreign_keys=[shipment_reference])
    decisions = relationship("FreightBillDecision", back_populates="freight_bill", order_by="FreightBillDecision.created_at")


class FreightBillDecision(Base):
    """Audit trail — every decision (agent or human) for a freight bill."""
    __tablename__ = "freight_bill_decisions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    freight_bill_id = Column(String, ForeignKey("freight_bills.id"), nullable=False)
    decision = Column(String, nullable=False)
    # auto_approved | flag_review | disputed | human_approved | human_disputed | duplicate | human_modified
    confidence_score = Column(Float)
    evidence = Column(JSON)          # full evidence chain from agent
    decided_by = Column(String, default="agent")  # "agent" or reviewer id
    notes = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    freight_bill = relationship("FreightBill", back_populates="decisions")
