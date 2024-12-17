import numpy as np
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Float, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import random
import time
import logging
from typing import List
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/postgres"

# SQLAlchemy setup
Base = declarative_base()


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)
    amount = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False)


class TransactionSimulator:
    def __init__(
        self,
        n_users: int = 10000,
        start_date: datetime = datetime.now() - timedelta(days=30),
        end_date: datetime = datetime.now(),
        churn_probability: float = 0.05,
        min_delay: float = 0.1,
        max_delay: float = 3.0,
    ):
        self.n_users = n_users
        self.start_date = start_date
        self.end_date = end_date
        self.churn_probability = churn_probability
        self.min_delay = min_delay
        self.max_delay = max_delay

        logger.info(f"Initializing simulator with {n_users} users")
        logger.info(f"Simulation period: {start_date} to {end_date}")
        logger.info(f"Delay range: {min_delay}s to {max_delay}s")

        try:
            # Initialize database
            self.engine = create_engine(DATABASE_URL)
            Base.metadata.create_all(self.engine)
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            logger.info("Successfully connected to database")
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise

        # Track active users
        self.active_users = set(range(1, n_users + 1))

        # Statistics tracking
        self.total_transactions = 0
        self.total_amount = 0
        self.failed_transactions = 0

    def generate_amount(self) -> float:
        """Generate transaction amount using mixture of distributions"""
        if random.random() < 0.95:
            # Regular purchases ($10-100)
            amount = round(np.random.lognormal(mean=3.5, sigma=0.5), 2)
        else:
            # Premium purchases ($100-10000)
            amount = round(np.random.lognormal(mean=6, sigma=1.0), 2)
        logger.debug(f"Generated transaction amount: ${amount}")
        return amount

    def generate_transaction_probability(self) -> float:
        """Generate daily transaction probability"""
        return random.uniform(1 / 7, 1)

    def insert_transaction(self, transaction: Transaction):
        """Insert a single transaction with retry logic"""
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                self.session.add(transaction)
                self.session.commit()
                self.total_transactions += 1
                self.total_amount += transaction.amount
                logger.info(
                    f"Transaction inserted: User {transaction.user_id}, Amount ${transaction.amount:.2f}, Time {transaction.timestamp}"
                )
                return True
            except SQLAlchemyError as e:
                retry_count += 1
                logger.warning(
                    f"Transaction insert failed (attempt {retry_count}/{max_retries}): {str(e)}"
                )
                self.session.rollback()
                time.sleep(1)  # Wait before retry

        self.failed_transactions += 1
        logger.error(f"Failed to insert transaction after {max_retries} attempts")
        return False

    def simulate_day(self, current_date: datetime):
        """Simulate transactions for a single day"""
        logger.info(f"Simulating transactions for {current_date.date()}")
        churned_users = set()

        for user_id in self.active_users:
            # Determine if user makes a purchase today
            if random.random() < self.generate_transaction_probability():
                # Generate random time during the day
                hours = random.randint(0, 23)
                minutes = random.randint(0, 59)
                timestamp = current_date.replace(hour=hours, minute=minutes)

                # Create and insert transaction
                transaction = Transaction(
                    user_id=user_id, amount=self.generate_amount(), timestamp=timestamp
                )

                # Insert with random delay
                delay = random.uniform(self.min_delay, self.max_delay)
                time.sleep(delay)
                self.insert_transaction(transaction)

                # Check for churn
                if random.random() < self.churn_probability:
                    churned_users.add(user_id)
                    logger.info(f"User {user_id} has churned")

        # Remove churned users
        self.active_users -= churned_users
        logger.info(
            f"Day completed. {len(churned_users)} users churned. {len(self.active_users)} active users remaining"
        )

    def run_simulation(self):
        """Run the complete simulation"""
        logger.info("Starting simulation")
        start_time = time.time()
        current_date = self.start_date

        try:
            while current_date <= self.end_date:
                self.simulate_day(current_date)
                current_date += timedelta(days=1)

                # Log daily statistics
                logger.info(
                    f"Daily Summary - Transactions: {self.total_transactions}, "
                    f"Total Amount: ${self.total_amount:.2f}, "
                    f"Active Users: {len(self.active_users)}"
                )

        except Exception as e:
            logger.error(f"Simulation failed: {str(e)}")
            raise
        finally:
            duration = time.time() - start_time
            logger.info(f"Simulation ended. Duration: {duration:.2f} seconds")
            self.session.close()

    def get_statistics(self):
        """Get basic statistics about the simulation"""
        try:
            stats = {
                "total_transactions": self.total_transactions,
                "failed_transactions": self.failed_transactions,
                "total_amount": round(self.total_amount, 2),
                "average_amount": (
                    round(self.total_amount / self.total_transactions, 2)
                    if self.total_transactions > 0
                    else 0
                ),
                "final_active_users": len(self.active_users),
                "simulation_period": f"{self.start_date.date()} to {self.end_date.date()}",
            }
            logger.info("Final Statistics: " + str(stats))
            return stats
        except Exception as e:
            logger.error(f"Failed to generate statistics: {str(e)}")
            raise


if __name__ == "__main__":
    try:
        # Example usage
        simulator = TransactionSimulator(
            n_users=1000,
            start_date=datetime.now() - timedelta(days=30),
            end_date=datetime.now(),
            min_delay=0.01,
            max_delay=0.1,
        )

        simulator.run_simulation()
        stats = simulator.get_statistics()

        logger.info("\nSimulation Statistics:")
        for key, value in stats.items():
            logger.info(f"{key}: {value}")

    except Exception as e:
        logger.error(f"Program failed: {str(e)}")
        raise
