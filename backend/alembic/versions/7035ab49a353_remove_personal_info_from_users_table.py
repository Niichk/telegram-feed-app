"""Remove personal info from users table

Revision ID: 7035ab49a353
Revises: a5138a59b6dd
Create Date: 2025-07-21 01:11:37.288059

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7035ab49a353'
down_revision: Union[str, Sequence[str], None] = 'a5138a59b6dd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('users', 'first_name')
    op.drop_column('users', 'username')
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('users', sa.Column('username', sa.VARCHAR(length=150), autoincrement=False, nullable=True))
    op.add_column('users', sa.Column('first_name', sa.VARCHAR(length=150), autoincrement=False, nullable=True))
    # ### end Alembic commands ###
