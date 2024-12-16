import json
import logging
import math
import sys
from datetime import datetime, timedelta
from typing import Any, Dict
import numpy as np
from scipy.special import expit

from confluent_kafka import Consumer, KafkaError
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    create_engine,
    func,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import declarative_base, sessionmaker

# Database configuration
DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/postgres"

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


Base = declarative_base()


class RFM(Base):
    """RFM metrics table with BG/NBD model parameters"""

    __tablename__ = "rfm"

    # Primary key
    user_id = Column(Integer, primary_key=True)

    # Basic RFM metrics
    first_purchase_date = Column(DateTime, nullable=False)
    last_purchase_date = Column(DateTime, nullable=False)
    frequency = Column(Integer, nullable=False)
    total_order_value = Column(Float, nullable=False)
    avg_order_value = Column(Float, nullable=False)
    retention_campaign_target_date = Column(DateTime, nullable=True)

    # Timestamp for last update - this is the improved version
    last_updated = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        server_onupdate=func.now(),  # This is PostgreSQL-specific
    )

    # # BG/NBD model parameters
    # r = Column(Float, nullable=True)
    # alpha = Column(Float, nullable=True)
    # a = Column(Float, nullable=True)
    # b = Column(Float, nullable=True)

    # Index definition
    __table_args__ = (
        Index("idx_rfm_dates", "first_purchase_date", "last_purchase_date"),
    )

    def __repr__(self):
        return (
            f"<RFM(user_id={self.user_id}, "
            f"frequency={self.frequency}, "
            f"total_order_value={self.total_order_value}, "
            f"retention_campaign_target_date={self.retention_campaign_target_date})>"
        )


class RFMDebeziumConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topics: list[str],
        auto_offset_reset: str = "earliest",
    ):
        """Initialize the RFM Debezium consumer"""
        self.topics = topics
        self.running = False

        # Configure Kafka consumer
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
                "enable.auto.commit": False,
            }
        )

        # Initialize database connection
        self.engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        # Pull these from somewhere at initialization time
        self.model_params = {
            # "r": 0.5, "beta": 0.7
            "r": 5.449462,
            "alpha": 9.599969,
            "a": 1.137758,
            "beta": 17.670075,
            "b": 17.670075,
        }

    def find_age_below_probability(
        self,
        frequency,
        recency,
        target_probability=0.7,
        max_age=1000,  # Reasonable upper bound
        tolerance=1e-6,  # Precision of probability calculation
    ):
        """
        Find the age at which the conditional probability of being alive
        falls below the target probability.

        Parameters
        ----------
        frequency: int or float
            Historical frequency of customer transactions.
        recency: int or float
            Historical recency of customer's last transaction.
        target_probability: float, optional (default=0.7)
            Probability threshold to find age for.
        max_age: int or float, optional (default=1000)
            Maximum age to search up to.
        tolerance: float, optional (default=1e-6)
            Precision for probability calculation.

        Returns
        -------
        float
            Age at which probability falls below target probability.
        """
        # Retrieve model parameters
        r = self.model_params["r"]
        alpha = self.model_params["alpha"]
        a = self.model_params["a"]
        b = self.model_params["b"]

        def _conditional_probability_alive(T):
            """
            Internal method to compute conditional probability alive at a given age.
            Matches the original implementation in the class method.
            """
            log_div = (r + frequency) * np.log(
                (alpha + T) / (alpha + recency)
            ) + np.log(a / (b + np.maximum(frequency, 1) - 1))
            return np.atleast_1d(np.where(frequency == 0, 1.0, expit(-log_div)))[0]

        # Binary search to find the age
        left, right = recency, max_age
        while right - left > tolerance:
            mid = (left + right) / 2
            prob = _conditional_probability_alive(mid)

            if prob > target_probability:
                left = mid
            else:
                right = mid

        return right if right < max_age else None

    def update_rfm_metrics(self, user_id: int, amount: float, timestamp: datetime):
        """
        Update RFM metrics for a user based on a new transaction.
        Uses UPSERT pattern to handle both new and existing users.
        """
        try:
            # Get current RFM values if they exist
            current_rfm = (
                self.session.query(
                    RFM.user_id,
                    RFM.first_purchase_date,
                    RFM.last_purchase_date,
                    RFM.frequency,
                    RFM.total_order_value,
                    RFM.avg_order_value,
                    RFM.retention_campaign_target_date,
                    func.floor(
                        func.extract(
                            "epoch", func.current_timestamp() - RFM.first_purchase_date
                        )
                        / 86400
                    ).label("recency_days"),
                    func.floor(
                        func.extract(
                            "epoch", func.current_timestamp() - RFM.last_purchase_date
                        )
                        / 86400
                    ).label("T_days"),
                )
                .filter(RFM.user_id == user_id)
                .first()
            )

            if current_rfm:
                # Update existing metrics
                new_frequency = current_rfm.frequency + 1
                new_total_order_value = current_rfm.total_order_value + amount
                new_avg_order = new_total_order_value / new_frequency
                new_retention_campaign_target_date = None

                target_age = self.find_age_below_probability(
                    current_rfm.frequency + 1, current_rfm.recency_days
                )
                if target_age:
                    new_retention_campaign_target_date = (
                        current_rfm.first_purchase_date
                        + timedelta(minutes=int(target_age * 24 * 60))
                    )

                stmt = insert(RFM).values(
                    user_id=user_id,
                    first_purchase_date=current_rfm.first_purchase_date,
                    last_purchase_date=timestamp,
                    frequency=new_frequency,
                    total_order_value=new_total_order_value,
                    avg_order_value=new_avg_order,
                    retention_campaign_target_date=new_retention_campaign_target_date,
                    last_updated=datetime.now(),
                )
                # logger.info(
                #     f"{user_id}: {timestamp} {new_frequency} {new_total_order_value} {new_avg_order}"
                # )

                stmt = stmt.on_conflict_do_update(
                    index_elements=["user_id"],
                    set_={
                        "last_purchase_date": stmt.excluded.last_purchase_date,
                        "frequency": stmt.excluded.frequency,
                        "total_order_value": stmt.excluded.total_order_value,
                        "avg_order_value": stmt.excluded.avg_order_value,
                        "retention_campaign_target_date": stmt.excluded.retention_campaign_target_date,
                        "last_updated": stmt.excluded.last_updated,
                    },
                )
            else:
                # Create new RFM entry
                stmt = insert(RFM).values(
                    user_id=user_id,
                    first_purchase_date=timestamp,
                    last_purchase_date=timestamp,
                    frequency=0,
                    total_order_value=amount,
                    avg_order_value=amount,
                    prediction_chance_alive=None,
                    last_updated=datetime.now(),
                )

            self.session.execute(stmt)
            self.session.commit()

            if current_rfm:
                logger.info(
                    f"Updated RFM metrics for user {user_id}: {current_rfm.prediction_chance_alive}"
                )
            else:
                logger.info(f"Updated RFM metrics for user {user_id}")

        except Exception as e:
            logger.error(f"Error updating RFM metrics for user {user_id}: {str(e)}")
            __import__("ipdb").set_trace()
            self.session.rollback()
            exit()
            raise

    def process_transaction(self, payload: Dict[str, Any]):
        """Process a transaction event from Debezium"""
        try:
            user_id = payload.get("user_id")
            amount = payload.get("amount")
            timestamp = payload.get("timestamp")

            if not all([user_id, amount, timestamp]):
                logger.warning("Missing required transaction fields")
                return

            # Parse timestamp

            timestamp = datetime.fromtimestamp(timestamp / 1000000)

            # Update RFM metrics
            self.update_rfm_metrics(user_id, amount, timestamp)

        except Exception as e:
            logger.error(f"Error processing transaction: {str(e)}")

    def start(self):
        """Start consuming messages"""
        try:
            self.consumer.subscribe(self.topics)
            logger.info(f"Subscribed to topics: {', '.join(self.topics)}")
            self.running = True

            while self.running:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Kafka error: {msg.error()}")
                    continue

                try:
                    # Parse and process the message
                    value = json.loads(msg.value().decode("utf-8"))
                    # print("Received message:")
                    # print(f"Topic: {msg.topic()}")
                    # print(f"Partition: {msg.partition()}")
                    # print(f"Offset: {msg.offset()}")
                    # # print(f"Key: {msg.key().decode('utf-8') if msg.key() else None}")
                    # # print(f"Value: {json.dumps(value, indent=2)}")
                    # print("-" * 50)
                    # print(value.get("payload"))
                    # print("-" * 50)

                    payload = value.get("payload")
                    self.process_transaction(payload)

                    self.consumer.commit(msg)

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse message: {str(e)}")
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")

        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")
        finally:
            logger.info("Closing consumer...")
            self.session.close()
            self.consumer.close()

    def get_rfm_statistics(self):
        """Get summary statistics of RFM metrics"""
        try:
            stats = {
                "total_users": self.session.query(func.count(RFM.user_id)).scalar(),
                "total_revenue": self.session.query(
                    func.sum(RFM.total_order_value)
                ).scalar(),
                "avg_frequency": self.session.query(func.avg(RFM.frequency)).scalar(),
                "avg_order_value": self.session.query(
                    func.avg(RFM.avg_order_value)
                ).scalar(),
            }
            return stats
        except Exception as e:
            logger.error(f"Error getting RFM statistics: {str(e)}")
            return None


if __name__ == "__main__":
    consumer = RFMDebeziumConsumer(
        bootstrap_servers="localhost:9092",
        group_id="rfm_processor",
        topics=["transactions-postgres.public.transactions"],
    )

    try:
        consumer.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)
