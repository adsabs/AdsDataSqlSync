"""create metrics table

Revision ID: 35972b7b1033
Revises: 
Create Date: 2016-07-26 14:11:35.655046

"""

# revision identifiers, used by Alembic.
revision = '35972b7b1033'
down_revision = None
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


def upgrade():
    op.create_table(
        'metrics',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('bibcode', sa.String, nullable=False, index=True, unique=True),
        sa.Column('refereed',sa.Boolean),
        sa.Column('rn_citations', postgresql.REAL),
        sa.Column('rn_citation_data', postgresql.JSON),
        sa.Column('downloads', postgresql.ARRAY(sa.Integer)),
        sa.Column('reads', postgresql.ARRAY(sa.Integer)),
        sa.Column('an_citations', postgresql.REAL),
        sa.Column('refereed_citation_num', sa.Integer),
        sa.Column('citation_num', sa.Integer),
        sa.Column('reference_num', sa.Integer),
        sa.Column('citations', postgresql.ARRAY(sa.String)),
        sa.Column('refereed_citations', postgresql.ARRAY(sa.String)),
        sa.Column('author_num', sa.Integer),
        sa.Column('an_refereed_citations', postgresql.REAL),
        sa.Column('modtime', sa.DateTime))


def downgrade():
    pass
