
# coding: utf-8

# # CREATE A RATIONAL DATABASE

# ## Explore the data

# In[1]:


import pandas as pd 
import csv
import sqlite3 

pd.set_option('max_columns', 180)
pd.set_option('max_rows', 200000)
pd.set_option('max_colwidth', 5000)

game = pd.read_csv('game_log.csv', low_memory=False)


# ### game_log 

# In[2]:


print(game.shape)
print(game['date'].min())
print(game['date'].max())
game.head()


# In[3]:


# Easy to make a reference to the explaination txt
gameColumns = game.columns.tolist()
for i, c in enumerate(gameColumns, 1):
    print(i, c)


# In[4]:


get_ipython().system('cat game_log_fields.txt')


# In[5]:


game.isnull().sum()


# There are more than 170,000 games since 1871. The game is played either as 'visiting' or 'home'. Every game has information on date, week, names of the teams, park, managers of each team.

# ### park_codes

# In[6]:


park = pd.read_csv('park_codes.csv')
print(park.shape)
park.head()


# ### person_codes

# In[7]:


person = pd.read_csv('person_codes.csv')
print(person.shape)
person.tail()


# There are about 20500 people involved in these games. Some people had two roles. 

# ### team_codes

# In[8]:


team = pd.read_csv('team_codes.csv')
print(team.shape)
team.head()


# In[9]:


team['franch_id'].value_counts().head()


# In[10]:


team[team['franch_id'] == 'BS1']


# Team moves between league and city with different team_id.

# ### Explore 'league'

# In[11]:


team['league'].value_counts()


# In[12]:


game['h_league'].value_counts()


# All teams belongs to 6 leagues. NL has largest number of teams, most games are played by NL and AL. 

# In[13]:


def league_years(league):
    rows = game[game['h_league']==league]
    start = rows['date'].min()
    end = rows['date'].max()
    print('{} went from {} to {}'.format(league, start, end))
    
for league in game['h_league'].dropna().unique():
    league_years(league)


# In[14]:


appearance_type = pd.read_csv('appearance_type.csv')
print(appearance_type.shape)
appearance_type.head()


# ## Prepare a Database 

# In[15]:


DB = 'BB.db'

def run_query(q):
    with sqlite3.connect(DB) as conn: 
        return pd.read_sql(q, conn)

    
def run_command(c):
    with sqlite3.connect(DB) as conn: 
        conn.execute('PRAGMA foreign_keys =ON;')
        conn.isolation_level = None
        conn.execute(c)
        

def show_table():
    q = '''
        SELECT 
            name,
            type
        FROM sqlite_master
        WHERE type IN ('table', 'view');   
    '''
    return run_query(q)


# ## Draft Database - import .CSV into .DB

# In[16]:


tables = {'game_log': game,
          'park_codes' :park,
          'person_codes': person,
          'team_codes': team,
          'appearance_type': appearance_type
         }

with sqlite3.connect(DB) as conn:
    for name, data in tables.items():
        conn.execute('DROP TABLE IF EXISTS {};'.format(name))
        data.to_sql(name, conn, index=False)


# In[17]:


show_table()


# ### Create a game_id 

# In[18]:


c1 = '''
ALTER TABLE game_log
ADD COLUMN game_id TEXT;
'''
try:
    run_command(c1)
except:
    pass 

c2 = '''
UPDATE game_log 
SET game_id = date || h_name || number_of_game
WHERE game_id IS NULL;
'''
run_command(c2)

q = '''
SELECT 
    game_id,
    date,
    h_name,
    number_of_game
FROM game_log 
LIMIT 5
'''
run_query(q)


# ## Database Normalization

# In database, create a few tables, each of them can be linked together.
# 
# __Part One__: 
# - `person`: person_id, last_name, first_name.
# - `park`: park_id, name, nickname/aka, city, state, notes.
# - `league`: league_id, name.
# - `appearance_type`: appearance_type_id, name, category
# 
# 
# __Part Two__: 
# - `team`: team_id, league_id, city, nickname, franch_id.
# - `game`: game_id, date, number_of_game, park_id, length_outs, day, completion, forefeit, protest, attendance, length_minutes, additional_info, acquisition_info.
# 
# 
# __Part Three__: 
# - `team_appearance`: team_id, game_id, home, league_id, score, line_score, at_bats, hits, doubles, triples, homeruns, rbi, scarifice_hits, scarifice_flies, hit_by_pitch, walks, intentional_walks, strikeouts, stolen_bases, caught_stealing, grounded_into_double, first_catcher_interference, left_on_base,   pitchers_used, individual_earned_runs, team_earned_run,  wild_pitches, balks, putouts, assists, errors, passed_balls, double_plays, triple_plays. 
# - notes: each team can be `v` team or `h` team.
# 
# 
# __Part Four__: 
# - `person_appearance` : appearance_id, person_id, team_id, game_id, appearance_type_id.
# - notes: personal_id, team_id, game_id, appearance_type_id are foreign keys, each of them linked to person(person_id), team(team_id), game(game_id), appearance_type(appearance_type_id) respectively. 

# ## Part One

# ### Table_1: <font color ='red'> person </font> 

# In[19]:


c1 = '''
    CREATE TABLE IF NOT EXISTS person (
        person_id TEXT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT
    );
'''

c2 = '''
    INSERT OR IGNORE INTO person
    SELECT
        id,
        first,
        last
    FROM person_codes;
'''

q = 'SELECT * FROM person LIMIT 5;'

run_command(c1)
run_command(c2)
run_query(q)


# ### Table_2:  <font color ='red'> park </font> 

# In[20]:


c0 = 'DROP TABLE IF EXISTS park;'
run_command(c0)

c1 = '''
CREATE TABLE IF NOT EXISTS park (
    park_id TEXT PRIMARY KEY,
    name TEXT,
    nickname TEXT,
    city TEXT,
    state TEXT,
    notes TEXT
);
'''

c2 = '''
INSERT OR IGNORE INTO park
    SELECT 
        park_id,
        name,
        aka,
        city,
        state,
        notes
    FROM park_codes;
'''

q = ' SELECT * FROM park LIMIT 5;'

run_command(c1)
run_command(c2)
run_query(q)


# ### Table_3:  <font color ='red'> league </font> (manually add into the table)

# In[21]:


c1 = '''
CREATE TABLE IF NOT EXISTS league (
    league_id TEXT PRIMARY KEY,
    name TEXT 
    );
'''

c2 = '''
INSERT OR IGNORE INTO league
VALUES 
    ("NL", "National League"),
    ("AL", "American League"),
    ("AA", "American Association"),
    ("FL", "Federal League"),
    ("PL", "Players League"),
    ("UA", "Union Association");
'''

q = 'SELECT * FROM league;'


run_command(c1)
run_command(c2)
run_query(q)


# ### Table_4:  <font color ='red'> appearance_type  </font> 

# In[22]:


# we have created appearance_type database in previouse step 
# ... (data.to_sql(name, conn, index=False))

q = 'SELECT * FROM appearance_type;'
run_query(q)


# In[23]:


c0 = 'DROP TABLE IF EXISTS appearance_type;'
run_command(c0)

c1 = '''
CREATE TABLE appearance_type (
appearance_type_id TEXT PRIMARY KEY,
name TEXT,
category TEXT
);
'''
run_command(c1)

with sqlite3.connect('BB.db') as conn:
    appearance_type.to_sql('appearance_type', conn, index =False, if_exists ='append')
    
q = 'SELECT * FROM appearance_type;'

run_query(q)
    


# __Note__ that although the two tables look identical, in practice, they are different. In the first table, we have not identified the primary key yet. It will inccur errors when we want to add data from it as a foreign key.

# In[24]:


show_table()


# ## Part Two: <font color ='green'> Team </font> and <font color ='green'> Game </font>

# ### Table_5: <font color = 'green'> team </font>

# In[25]:


c1 = '''
CREATE TABLE IF NOT EXISTS team (
    team_id TEXT PRIMARY KEY,
    league_id TEXT,
    city TEXT,
    nickname TEXT,
    franch_id TEXT
);
'''

c2 = '''
INSERT OR IGNORE INTO team
    SELECT
        team_id,
        league, 
        city,
        nickname,
        franch_id
    FROM team_codes;
'''

q = 'SELECT * FROM team LIMIT 5;'

run_command(c1)
run_command(c2)
run_query(q)


# ### Table_6 : <font color = 'green'> game </font>

# In[26]:


c1 = '''
CREATE TABLE IF NOT EXISTS game (
    game_id TEXT PRIMARY KEY,
    date INT,
    number_of_game INT,
    park_id TEXT,
    length_outs INT,
    day BOOLEAN,
    completion TEXT,
    forefeit TEXT,
    protest TEXT,
    attendance INT,
    length_minutes INT,
    additional_info TEXT,
    acquisition_info TEXT,
    FOREIGN KEY (park_id) REFERENCES park(park_id)
);
'''

c2  = '''
INSERT OR IGNORE INTO game
    SELECT
        game_id,
        date,
        number_of_game,
        park_id,
        length_outs,
        CASE
            WHEN day_night = 'D' THEN 1
            WHEN day_night = 'N' THEN 0
            ELSE NULL
            END
            AS day,
        completion,
        forefeit,
        protest,
        attendance,
        length_minutes,
        additional_info,
        acquisition_info
    FROM game_log;
'''

q = 'SELECT * FROM game LIMIT 5;'

run_command(c1)
run_command(c2)
run_query(q)


# In[27]:


show_table()


# ## Part Three: <font color = 'blue'> Team_performance </font>

# ### Table_7:<font color = 'blue'> team_performance </font>

# In[28]:


# add all info related to a team's perforamnce in a game,
# one team plays twice as h or v in a game.
c0 ='DROP TABLE IF EXISTS team_performance;'
run_command(c0)


c1 = '''
CREATE TABLE IF NOT EXISTS team_performance (
    team_id TEXT,
    game_id TEXT,
    home BOOLEAN,
    league_id TEXT,
    score INT,
    line_score TEXT,
    at_bats INT,
    hits INT,
    doubles INT,
    triples INT,
    homeruns INT,
    rbi INT,
    scarifice_hits INT,
    scarifice_files INT,
    hit_by_pitch INT,
    walks INT,
    intentional_walks INT,
    strikeouts INT,
    stolen_bases INT,
    caught_stealing INT,
    grounded_into_double INT,
    first_catcher_interference INT,
    left_on_base INT,
    pitchers_used INT,
    individual_earned_runs INT,
    team_earned_run INT,
    wild_pitches INT,
    balks INT,
    putouts INT,
    assists INT,
    errors INT,
    passed_balls INT,
    double_plays INT,
    triple_plays INT,
    PRIMARY KEY (team_id, game_id),
    FOREIGN KEY (team_id) REFERENCES team(team_id),
    FOREIGN KEY (game_id) REFERENCES game(game_id)
);
'''

# team(team_id ) refers to game_log(h_name)/game_log(v_log)
c2 = '''
INSERT OR IGNORE INTO team_performance
    SELECT
        h_name,
        game_id,
        1 as home,
        h_league,
        h_score,
        h_line_score,
        h_at_bats,
        h_hits,
        h_doubles,
        h_triples,
        h_homeruns,
        h_rbi,
        h_sacrifice_hits,
        h_sacrifice_flies,
        h_hit_by_pitch,
        h_walks,
        h_intentional_walks,
        h_strikeouts,
        h_stolen_bases,
        h_caught_stealing,
        h_grounded_into_double,
        h_first_catcher_interference,
        h_left_on_base,
        h_pitchers_used,
        h_individual_earned_runs,
        h_team_earned_runs,
        h_wild_pitches,
        h_balks,
        h_putouts,
        h_assists,
        h_errors,
        h_passed_balls,
        h_double_plays,
        h_triple_plays
   FROM game_log
   
UNION

    SELECT
        v_name,
        game_id,
        0 AS HOME,
        v_league,
        v_score,
        v_line_score,
        v_at_bats,
        v_hits,
        v_doubles,
        v_triples,
        v_homeruns,
        v_rbi,
        v_sacrifice_hits,
        v_sacrifice_flies,
        v_hit_by_pitch,
        v_walks,
        v_intentional_walks,
        v_strikeouts,
        v_stolen_bases,
        v_caught_stealing,
        v_grounded_into_double,
        v_first_catcher_interference,
        v_left_on_base,
        v_pitchers_used,
        v_individual_earned_runs,
        v_team_earned_runs,
        v_wild_pitches,
        v_balks,
        v_putouts,
        v_assists,
        v_errors,
        v_passed_balls,
        v_double_plays,
        v_triple_plays
    FROM game_log;  
'''

q = '''
SELECT * FROM team_performance
WHERE game_id = (
                SELECT MIN(game_id) FROM game
                )
   OR game_id = (
                SELECT MAX(game_id) FROM game
                )
ORDER BY game_id;
'''

run_command(c1)
run_command(c2)
run_query(q)


# ## Part Four: <font color ='Orange'> Person_appearance

# ### Table_8 : <font color ='Orange'> person_appearance

# Person in a game can have many types of permutations:
# - `h` / `v`, 2;
# - `offense`  / `defense`, 2;
# - `positions`, 9
# - 2 x 2 x 9 = 36

# In[29]:


c0 = 'DROP TABLE IF EXISTS person_appearance;'
run_command(c0)

c1 = '''
CREATE TABLE person_appearance (
    appearance_id INTEGER PRIMARY KEY,
    person_id TEXT,
    team_id TEXT,
    game_id TEXT,
    appearance_type_id TEXT,
    FOREIGN KEY (person_id) REFERENCES person(person_id),
    FOREIGN KEY (team_id) REFERENCES team(team_id),
    FOREIGN KEY (game_id) REFERENCES game(game_id),
    FOREIGN KEY (appearance_type_id) REFERENCES appearance_type(appearance_type_id)
);
'''

# the primary key is an integer, if we don't specify a value for this column
# when insecting rows, SQLite will autoincrement this column. 
# use [] when column start with a number 
# i.e use [1b_umpire_id] instead of 1b_umpire_id
# each _umpire_id, _manager_id, pitcher_id is associated with person_id

c2 = '''
INSERT OR IGNORE INTO person_appearance (
    game_id,
    team_id,
    person_id,
    appearance_type_id
)

    SELECT 
        game_id,
        NULL,       
        hp_umpire_id,
        "UHP"
    FROM game_log
    WHERE hp_umpire_id IS NOT NULL

UNION

    SELECT 
        game_id,
        NULL,
        [1b_umpire_id],
        "U1B"
    FROM game_log
    WHERE [1b_umpire_id] IS NOT NULL
    
UNION
    
    SELECT 
        game_id,
        NULL,
        [2b_umpire_id],
        "U2B"
    FROM game_log
    WHERE [2b_umpire_id] IS NOT NULL
    
UNION

    SELECT 
        game_id,
        NULL,
        [3b_umpire_id],
        "U3B"
    FROM game_log
    WHERE [3b_umpire_id] IS NOT NULL
    
UNION

    SELECT
        game_id,
        NULL,
        lf_umpire_id,
        "ULF"
    FROM game_log
    WHERE lf_umpire_id IS NOT NULL
    
UNION

    SELECT 
        game_id,
        NULL,
        rf_umpire_id,
        "URF"
    FROM game_log
    WHERE rf_umpire_id IS NOT NULL
    
UNION

    SELECT
        game_id,
        v_name,
        v_manager_id,
        "MM"
    FROM game_log
    WHERE v_manager_id IS NOT NULL
    
UNION

    SELECT
        game_id,
        h_name,
        h_manager_id,
        "MM"
    FROM game_log
    WHERE h_manager_id IS NOT NULL
    
UNION

    SELECT
        game_id,
        CASE
            WHEN h_score > v_score THEN h_name
            ELSE v_name
            END,
        winning_pitcher_id, 
        "AWP"
    FROM game_log
    WHERE winning_pitcher_id IS NOT NULL
    
UNION

    SELECT
        game_id,
        CASE
            WHEN h_score < v_score THEN h_name
            ELSE v_name
            END,
        losing_pitcher_id,
        "ALP"
    FROM game_log
    WHERE losing_pitcher_id IS NOT NULL
    
UNION

    SELECT
        game_id,
        CASE
            WHEN h_score > v_score THEN h_name
            ELSE v_name
            END,
        saving_pitcher_id,
        "ASP"
    FROM game_log
    WHERE saving_pitcher_id IS NOT NULL


UNION

    SELECT
        game_id,
        CASE
            WHEN h_score > v_score THEN h_name
            ELSE v_name
            END,
        winning_rbi_batter_id,
        "AWB"
    FROM game_log
    WHERE winning_rbi_batter_id IS NOT NULL
    

UNION
    
    SELECT
        game_id,
        v_name,
        v_starting_pitcher_id,
        "PSP"
    FROM game_log
    WHERE v_starting_pitcher_id IS NOT NULL
    
UNION

    SELECT
        game_id,
        h_name,
        h_starting_pitcher_id,
        "PSP"
    FROM game_log
    WHERE h_starting_pitcher_id IS NOT NULL;
'''

# Offense: h/v
# Deffense: h/v

template = '''
INSERT INTO person_appearance (
    game_id,
    team_id,
    person_id,
    appearance_type_id
)

    SELECT
        game_id,
        {hv}_name,
        {hv}_player_{num}_id,
        "O{num}"
    FROM game_log
    WHERE {hv}_player_{num}_id IS NOT NULL
    
UNION

    SELECT
        game_id,
        {hv}_name,
        {hv}_player_{num}_id,
        "D" || CAST({hv}_player_{num}_def_pos AS INT)
    FROM game_log
    WHERE {hv}_player_{num}_id IS NOT NULL;
'''

run_command(c1)
run_command(c2)


for hv in ["h", "v"]:
    for num in range(1, 10):
        q_vars = {"hv": hv, "num": num}
        run_command(template.format(**q_vars))


# In[30]:


q1 = 'SELECT COUNT(DISTINCT game_id) game_gameN from game;'
q2 = 'SELECT COUNT(DISTINCT game_id) person_appear_gameN FROM person_appearance;'

print(run_query(q1))
print(run_query(q2))


# In[31]:


q = 'SELECT * FROM person_appearance LIMIT 5;'

run_query(q)


# In[32]:


q = '''
SELECT 
    pa.*,
    at.name,
    at.category
FROM person_appearance pa
INNER JOIN appearance_type at ON at.appearance_type_id = pa.appearance_type_id
WHERE pa.game_id = (
                    SELECT MAX(game_id)
                    FROM person_appearance
                    )
ORDER BY team_id, appearance_type_id;
'''
run_query(q)


# In[33]:


show_table()


# ## Remove non-use tables

# In[34]:


tables = [
        'game_log',
        'park_codes',
        'person_codes',
        'team_codes'    
]

for t in tables:
    c  = 'DROP TABLE {}'.format(t)
    run_command(c)
    
    
    
show_table()

