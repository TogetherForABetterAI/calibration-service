from sqlalchemy.exc import NoResultFound, SQLAlchemyError, IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from sqlalchemy import select, update, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
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


    def get_latest_scores_record(self, session_id) -> Scores | None:
        """
        Retrieve the latest Scores record for a given session_id.
        Returns the Scores record if found, otherwise None.
        """
        with Session(self.engine) as session:
            try:
                stmt = select(Scores).where(
                    Scores.session_id == session_id
                )
                result = session.execute(stmt).scalar_one_or_none()

                if result:
                    logging.info(f"Scores record found for session_id: {session_id}")
                    return result
                else:
                    logging.info(f"No Scores record found for session_id: {session_id}")
                    return None
            except SQLAlchemyError as e:
                logging.error(f"Error retrieving Scores record for session_id {session_id}: {e}")
                return None

    def update_session_state(self, session_id, updates):
        with Session(self.engine) as session:
            try: 
                stmt = update(Scores).where(
                    Scores.session_id == session_id
                )
                
                values = {}
                
                if 'push_alphas' in updates:
                    values['alphas'] = func.array_append(Scores.alphas, updates['push_alphas'])
                if 'push_uncertainties' in updates:
                    values['uncertainties'] = func.array_append(Scores.uncertainties, updates['push_uncertainties'])
                if 'push_coverages' in updates:
                    values['coverages'] = func.array_append(Scores.coverages, updates['push_coverages'].item())
                if 'push_setsizes' in updates:
                    values['setsizes'] = func.array_append(Scores.setsizes, updates['push_setsizes'])
                if 'push_confidences' in updates:
                    values['confidences'] = func.coalesce(Scores.confidences, b'').op('||')(updates['push_confidences'])

                # Actualizaciones escalares
                if 'accuracy' in updates:
                    values['accuracy'] = updates['accuracy']
                if 'correct_preds' in updates:
                    values['correct_preds'] = updates['correct_preds']
                if 'total_samples' in updates:
                    values['total_samples'] = updates['total_samples']

                # Variables del uq
                if 'alpha' in updates:
                    values['alpha'] = updates['alpha']
                if 'q_hat' in updates:
                    values['q_hat'] = updates['q_hat']
                if 'scores' in updates:
                    values['scores'] = updates['scores']
    
                                
                values['batchs_counter'] = updates['batchs_counter']
                values['stage'] = updates['stage']
                
                stmt = stmt.values(values)
                session.execute(stmt)
                session.commit()
            except SQLAlchemyError as e:
                logging.error(f"Error updating session state for session_id {session_id}: {e}")
                session.rollback()

    def create_scores_record(self, session_id):
            with Session(self.engine) as session:
                try:
                    stmt = pg_insert(Scores).values(
                        session_id=session_id,
                        alphas=[],          
                        uncertainties=[],
                        coverages=[],
                        setsizes=[],
                        confidences=b'',
                    )
                    
                    stmt = stmt.on_conflict_do_nothing(
                        index_elements=['session_id']
                    )
                    
                    session.execute(stmt)
                    session.commit()
                    
                except SQLAlchemyError as e:
                    session.rollback()
                    logging.error(f"Error creating scores record for {session_id}: {e}")
                    raise e

    def write_inputs(self, session_id: UUID, inputs: bytes, batch_index: int):
        """
        Write inputs to the database for a given session_id and batch_index.
        If a record with the session_id and batch_index exists, update it; otherwise, insert a new record
        """
        with Session(self.engine) as session:
            try:
                stmt = select(ModelInputs).where(
                    ModelInputs.session_id == session_id,
                    ModelInputs.batch_index == batch_index
                )
                result = session.execute(stmt).scalar_one_or_none()

                if result:
                    result.inputs = inputs
                    logging.info(f"Updating inputs for session_id: {session_id}, batch_index: {batch_index}")
                else:
                    new_input = ModelInputs(session_id=session_id, batch_index=batch_index, inputs=inputs)
                    session.add(new_input)
                    logging.info(f"Inserting new inputs for session_id: {session_id}, batch_index: {batch_index}")

                session.commit()
            except SQLAlchemyError as e:
                logging.error(f"Error writing inputs for session_id {session_id}, batch_index {batch_index}: {e}")
                session.rollback()

    def write_outputs(self, session_id: UUID, outputs: bytes, batch_index: int):
        """
        Write outputs to the database for a given session_id and batch_index.
        If a record with the session_id and batch_index exists, update it; otherwise, insert a new record
        """
        with Session(self.engine) as session:
            try:
                stmt = select(ModelOutputs).where(
                    ModelOutputs.session_id == session_id,
                    ModelOutputs.batch_index == batch_index
                )
                result = session.execute(stmt).scalar_one_or_none()

                if result:
                    result.outputs = outputs
                    logging.info(f"Updating outputs for session_id: {session_id}, batch_index: {batch_index}")
                else:
                    new_output = ModelOutputs(session_id=session_id, batch_index=batch_index, outputs=outputs)
                    session.add(new_output)
                    logging.info(f"Inserting new outputs for session_id: {session_id}, batch_index: {batch_index}")
                session.commit()
            except SQLAlchemyError as e:
                logging.error(f"Error writing outputs for session_id {session_id}, batch_index {batch_index}: {e}")
                session.rollback()
    
    def get_inputs_from_session(self, session_id: UUID) -> list[bytes] | None:
        """
        Read inputs from the database for a given session_id.
        Returns a list of inputs as bytes if found, otherwise None.
        """
        with Session(self.engine) as session:
            try:
                stmt = select(ModelInputs).where(ModelInputs.session_id == session_id)
                results = session.execute(stmt).scalars().all()

                if results:
                    logging.info(f"Inputs found for session_id: {session_id}")
                    return [result.inputs for result in results]
                else:
                    logging.info(f"No inputs found for session_id: {session_id}")
                    return []
            except SQLAlchemyError as e:
                logging.error(f"Error reading inputs for session_id {session_id}: {e}")
                return None
        
            

    def get_outputs_from_session(self, session_id: UUID) -> list[bytes] | None:
        """
        Read outputs from the database for a given session_id.
        Returns a list of outputs as bytes if found, otherwise None.
        """
        with Session(self.engine) as session:
            try:
                stmt = select(ModelOutputs).where(ModelOutputs.session_id == session_id)
                results = session.execute(stmt).scalars().all()

                if results:
                    logging.info(f"Outputs found for session_id: {session_id}")
                    return [result.outputs for result in results]
                else:
                    logging.info(f"No outputs found for session_id: {session_id}")
                    return []
            except SQLAlchemyError as e:
                logging.error(f"Error reading outputs for session_id {session_id}: {e}")
                return None
                
            
        

      


  