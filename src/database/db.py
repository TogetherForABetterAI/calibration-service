from sqlalchemy import create_engine, MetaData, Table, Column, String, or_, and_
from sqlalchemy.exc import NoResultFound, SQLAlchemyError, IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from sqlalchemy import select


from datetime import datetime, timedelta, timezone
from uuid import UUID
import logging
from src.models.inputs import ModelInputs
from src.models.outputs import ModelOutputs
from src.models.scores import Scores
from src.lib.db_engine import Base


class Database:
    def __init__(self, engine):
        self.engine = engine
        self.scores_table = Scores.__table__
        self.inputs_table = ModelInputs.__table__
        self.outputs_table = ModelOutputs.__table__
        self.create_table()

    def create_table(self):
        with self.engine.connect() as connection:
            try:
                Base.metadata.create_all(bind=connection) 
                logging.info("Table created successfully")
                connection.commit()
            except SQLAlchemyError as e:
                logging.error(f"Error creating table: {e}")
                connection.rollback()

            table_user_exists = connection.dialect.has_table(connection, "users") 
            if table_user_exists:
                logging.info("Table 'users' exists")
            else:
                logging.info("Table does not exist")

            if connection.dialect.has_table(connection, "userinfo"):
                logging.info("Table 'userinfo' exists")
            else:
                logging.info("Table 'userinfo' does not exist")

            if connection.dialect.has_table(connection, "followers"):
                logging.info("Table 'followers' exists")
            else:
                logging.info("Table 'followers' does not exist")

    def update_scores(self, session_id: UUID, scores: bytes):
        """
        Update scores to the database for a given session_id.
        If a record with the session_id exists, update it; otherwise, insert a new record
        """
        with Session(self.engine) as session:
            try:
                stmt = select(Scores).where(Scores.session_id == session_id)
                result = session.execute(stmt).scalar_one_or_none()

                if result:
                    result.scores = scores
                    logging.info(f"Updating scores for session_id: {session_id}")
                else:
                    new_score = Scores(session_id=session_id, scores=scores)
                    session.add(new_score)
                    logging.info(f"Inserting new scores for session_id: {session_id}")

                session.commit()
            except SQLAlchemyError as e:
                logging.error(f"Error writing scores for session_id {session_id}: {e}")
                session.rollback()
            finally:
                session.close()

    def write_inputs(self, session_id: UUID, inputs: bytes):
        """
        Write inputs to the database for a given session_id.
        If a record with the session_id exists, update it; otherwise, insert a new record
        """
        with Session(self.engine) as session:
            try:
                stmt = select(ModelInputs).where(ModelInputs.session_id == session_id)
                result = session.execute(stmt).scalar_one_or_none()

                if result:
                    result.inputs = inputs
                    logging.info(f"Updating inputs for session_id: {session_id}")
                else:
                    new_input = ModelInputs(session_id=session_id, inputs=inputs)
                    session.add(new_input)
                    logging.info(f"Inserting new inputs for session_id: {session_id}")

                session.commit()
            except SQLAlchemyError as e:
                logging.error(f"Error writing inputs for session_id {session_id}: {e}")
                session.rollback()
            finally:
                session.close()

    def write_outputs(self, session_id: UUID, outputs: bytes):
        """
        Write outputs to the database for a given session_id.
        If a record with the session_id exists, update it; otherwise, insert a new record
        """
        with Session(self.engine) as session:
            try:
                stmt = select(ModelOutputs).where(ModelOutputs.session_id == session_id)
                result = session.execute(stmt).scalar_one_or_none()

                if result:
                    result.outputs = outputs
                    logging.info(f"Updating outputs for session_id: {session_id}")
                else:
                    new_output = ModelOutputs(session_id=session_id, outputs=outputs)
                    session.add(new_output)
                    logging.info(f"Inserting new outputs for session_id: {session_id}")

                session.commit()
            except SQLAlchemyError as e:
                logging.error(f"Error writing outputs for session_id {session_id}: {e}")
                session.rollback()
            finally:
                session.close()
    
    def get_scores_from_session(self, session_id: UUID) -> bytes | None:
        """
        Read scores from the database for a given session_id.
        Returns the scores as bytes if found, otherwise None.
        """
        with Session(self.engine) as session:
        
            try:
                stmt = select(Scores).where(Scores.session_id == session_id)
                result = session.execute(stmt).scalar_one_or_none()

                if result:
                    logging.info(f"Scores found for session_id: {session_id}")
                    return result.scores
                else:
                    logging.info(f"No scores found for session_id: {session_id}")
                    return None
            except SQLAlchemyError as e:
                logging.error(f"Error reading scores for session_id {session_id}: {e}")
                return None
            finally:
                session.close()

    def get_inputs_from_session(self, session_id: UUID) -> bytes | None:
        """
        Read inputs from the database for a given session_id.
        Returns the inputs as bytes if found, otherwise None.
        """
        with Session(self.engine) as session:
            try:
                stmt = select(ModelInputs).where(ModelInputs.session_id == session_id)
                result = session.execute(stmt).scalar_one_or_none()

                if result:
                    logging.info(f"Inputs found for session_id: {session_id}")
                    return result.inputs
                else:
                    logging.info(f"No inputs found for session_id: {session_id}")
                    return None
            except SQLAlchemyError as e:
                logging.error(f"Error reading inputs for session_id {session_id}: {e}")
                return None
            finally:
                session.close()

    def get_outputs_from_session(self, session_id: UUID) -> bytes | None:
        """
        Read outputs from the database for a given session_id.
        Returns the outputs as bytes if found, otherwise None.
        """
        with Session(self.engine) as session:
            try:
                stmt = select(ModelOutputs).where(ModelOutputs.session_id == session_id)
                result = session.execute(stmt).scalar_one_or_none()

                if result:
                    logging.info(f"Outputs found for session_id: {session_id}")
                    return result.outputs
                else:
                    logging.info(f"No outputs found for session_id: {session_id}")
                    return None
            except SQLAlchemyError as e:
                logging.error(f"Error reading outputs for session_id {session_id}: {e}")
                return None
            finally:
                session.close()


    def get_last_processed_batch_id(self) -> int | None:
        """
        Retrieve the last processed batch ID from the database.
        Returns the batch ID as an integer if found, otherwise None.
        """
        with Session(self.engine) as session:
            try:
                stmt = select(Scores).order_by(Scores.batch_id.desc()).limit(1)
                result = session.execute(stmt).scalar_one_or_none()

                if result:
                    logging.info(f"Last processed batch ID: {result.batch_id}")
                    return result.batch_id
                else:
                    logging.info("No processed batches found")
                    return None
            except SQLAlchemyError as e:
                logging.error(f"Error retrieving last processed batch ID: {e}")
                return None
            finally:
                session.close()
                
            
        

      


  