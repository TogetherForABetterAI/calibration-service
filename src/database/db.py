from sqlalchemy.exc import NoResultFound, SQLAlchemyError, IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from sqlalchemy import select
from uuid import UUID
import logging
from src.models.inputs import ModelInputs
from src.models.outputs import ModelOutputs
from src.models.scores import Scores
from src.lib.db_engine import Base

class Database:
    def __init__(self, engine):
        self.engine = engine
        try:
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            Base.metadata.create_all(bind=self.engine)

            logging.info("Tables created successfully")
            logging.info(f"USING ENGINE: {self.engine.url}")
            logging.info(Base.metadata.tables.keys())

        except SQLAlchemyError as e:
            logging.error(f"Error creating tables: {e}")

            
    def update_vec_scores(self, session_id: UUID, vec_scores: bytes):
        """
        Update vec_scores to the database for a given session_id.
        If a record with the session_id exists, update it; otherwise, insert a new record
        """
        with Session(self.engine) as session:
            try:
                stmt = select(Scores).where(Scores.session_id == session_id)
                result = session.execute(stmt).scalar_one_or_none()

                if result:
                    result.vec_scores = vec_scores
                    logging.info(f"Updating vec_scores for session_id: {session_id}")
                else:
                    new_score = Scores(session_id=session_id, vec_scores=vec_scores)
                    session.add(new_score)
                    logging.info(f"Inserting new vec_scores for session_id: {session_id}")

                session.commit()
            except SQLAlchemyError as e:
                logging.error(f"Error writing vec_scores for session_id {session_id}: {e}")
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
        Read vec_scores from the database for a given session_id.
        Returns the vec_scores as bytes if found, otherwise None.
        """
        with Session(self.engine) as session:
        
            try:
                stmt = select(Scores).where(Scores.session_id == session_id)
                result = session.execute(stmt).scalar_one_or_none()

                if result:
                    logging.info(f"Scores found for session_id: {session_id}")
                    return result.vec_scores
                else:
                    logging.info(f"No vec_scores found for session_id: {session_id}")
                    return []
            except SQLAlchemyError as e:
                logging.error(f"Error reading vec_scores for session_id {session_id}: {e}")
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
                    return []
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
                    return []
            except SQLAlchemyError as e:
                logging.error(f"Error reading outputs for session_id {session_id}: {e}")
                return None
            finally:
                session.close()


    def get_last_processed_batch_index(self) -> int | None:
        """
        Retrieve the last processed batch ID from the database.
        Returns the batch ID as an integer if found, otherwise None.
        """
        with Session(self.engine) as session:
            try:
                stmt = select(Scores).order_by(Scores.batch_index.desc()).limit(1)
                result = session.execute(stmt).scalar_one_or_none()

                if result:
                    logging.info(f"Last processed batch ID: {result.batch_index}")
                    return result.batch_index
                else:
                    logging.info("No processed batches found")
                    return None
            except SQLAlchemyError as e:
                logging.error(f"Error retrieving last processed batch ID: {e}")
                return None
            finally:
                session.close()
                
            
        

      


  