-- Separate databases for the Iceberg catalog and Airflow metadata.
CREATE DATABASE iceberg;
CREATE DATABASE airflow;

-- The booking system's operational table (source of CDC).
CREATE TABLE public.bookings (
    booking_id   TEXT PRIMARY KEY,
    guest_id     TEXT        NOT NULL,
    listing_id   TEXT        NOT NULL,
    status       TEXT        NOT NULL CHECK (status IN ('PENDING', 'CONFIRMED', 'CANCELLED')),
    amount_cents BIGINT      NOT NULL CHECK (amount_cents > 0),
    currency     TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    confirmed_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Full row images on UPDATE/DELETE so CDC consumers never see partial rows.
ALTER TABLE public.bookings REPLICA IDENTITY FULL;
CREATE INDEX bookings_status_updated_idx ON public.bookings (status, updated_at);

-- Only this table is published for logical replication.
CREATE PUBLICATION reconstream_pub FOR TABLE public.bookings;

-- Ground truth written by the traffic generator. Not replicated.
CREATE SCHEMA audit;
CREATE TABLE audit.injected_faults (
    id                 BIGSERIAL PRIMARY KEY,
    booking_id         TEXT        NOT NULL,
    fault_type         TEXT        NOT NULL,
    expected_exception TEXT        NOT NULL,
    expect_resolved    BOOLEAN     NOT NULL DEFAULT FALSE,
    injected_at        TIMESTAMPTZ NOT NULL,
    detectable_at      TIMESTAMPTZ NOT NULL,
    details            JSONB,
    recorded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX injected_faults_booking_idx ON audit.injected_faults (booking_id);
