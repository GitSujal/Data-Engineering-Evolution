import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import json
import os
import sqlalchemy
import yaml


config = yaml.safe_load(open('config.yml'))
# create a new db engine
server = config['db_credentials']['server']
database = config['db_credentials']['database']
user = config['db_credentials']['user']
password = os.getenv('DP101_PASSWORD')

sql_engine = sqlalchemy.create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server",
                                  connect_args={'connect_timeout': 30})


st.set_page_config(page_title="Salary comparison for Data Engineers", page_icon="🔨", layout="wide")
st.title("Salary comparison for Data Engineers")

@st.cache_data
def load_data():
    """
    load the data from the database
    :return: dataframe
    """
    return pd.read_sql('SELECT * FROM jobs_processed', sql_engine)

df = load_data()


with st.sidebar:
    st.header("About")
    st.markdown(""" ### This is a simple app to scrape data from Seek.
    * you can use it to find the top skills for data professionals 
    * you can also use it to find the salaries for those roles
    Hope this helps you in your job search! 
    """)

# skills keywords list
with open('data/keywords/skill_keywords.json') as f:
    skills = json.load(f)
# programming languages keywords list
with open('data/keywords/programming_keywords.json') as f:
    langugages = json.load(f)

df['has_valid_salary'] = df['per_annum'].apply(lambda x: "Salary Mentioned" if x >= 50000 and x <= 500000 else "Salary Not Mentioned")
# produce a frequency distribution of the salary where the salary is mentioned
valid_sals = df[df['has_valid_salary'] == 'Salary Mentioned']

avg, high, skill ,lang =  st.tabs(["Average Salary", "Higher Salary", "Salary by Skill", "Salary by Languages"])

with avg:
    # average per_annum by job title where the count is at least 5 and the job title contains engineer
    eng_sal = valid_sals[valid_sals['jobTitle'].str.contains('engineer', case=False)]
    sal_avg = valid_sals.groupby('jobTitle')['per_annum'].mean().reset_index()
    eng_count = eng_sal.groupby('jobTitle')['per_annum'].count().reset_index()
    eng_count = eng_count[eng_count['per_annum'] > 5]
    sal_avg = sal_avg.merge(eng_count, on='jobTitle')
    sal_avg = sal_avg.sort_values(by='per_annum_x', ascending=True)
    sal_avg = sal_avg.tail(30)
    fig = go.Figure(data=[go.Bar(y=sal_avg['jobTitle'], x=sal_avg['per_annum_x'],
                                orientation='h', text=sal_avg['per_annum_x'],
                                textposition='outside',
                                showlegend=False,
                                texttemplate='%{text:.3s}'
                                )
                                ])

    # update the x axis range to 50000 to 200000
    fig.update_xaxes(range=[50000, 200000])

    # increase the font size and make it black and bold
    fig.update_layout(font=dict(size=12, color='black'))
    fig.update_layout(title_text='Average Salary range for Data engineers')
    fig.update_layout(width=1280, height=720)
    # show the chart
    st.plotly_chart(fig, use_container_width=True, sharing='streamlit', config={'displaylogo': False}, theme='streamlit')
    # show some stats
    st.info("Above chart is based on {} jobs scraped from Seek".format(len(valid_sals)), icon="ℹ️")

with high:
    eng_sal = valid_sals[valid_sals['jobTitle'].str.contains('data', case=False)]
    sal_avg = valid_sals.groupby('jobTitle')['per_annum'].mean().reset_index()
    eng_count = eng_sal.groupby('jobTitle')['per_annum'].count().reset_index()
    eng_count = eng_count[eng_count['per_annum'] < 5]
    sal_avg = sal_avg[sal_avg['per_annum'] > 300000]
    sal_avg = sal_avg.merge(eng_count, on='jobTitle')
    sal_avg = sal_avg.sort_values(by='per_annum_x', ascending=True)
    # only keep the job title to 30 characters
    sal_avg['jobTitle'] = sal_avg['jobTitle'].apply(lambda x: x[:30])
    sal_avg = sal_avg.tail(30)
    fig = go.Figure(data=[go.Bar(y=sal_avg['jobTitle'], x=sal_avg['per_annum_x'],
                                orientation='h', text=sal_avg['per_annum_x'],
                                textposition='outside',
                                showlegend=False,
                                texttemplate='%{text:.3s}'
                                )
                                ])
    # increase the font size and make it black and bold
    fig.update_layout(font=dict(size=12, color='black'))
    fig.update_layout(title_text='Higher Salary Range for Data Engineers')
    # update the x axis range to 200000 to 200000
    fig.update_xaxes(range=[200000, 400000])

    fig.update_layout(width=1280, height=720)
    # show the chart
    st.plotly_chart(fig, use_container_width=True, sharing='streamlit', config={'displaylogo': False}, theme='streamlit')
    # show some stats
    st.info("Above chart is based on {} jobs scraped from Seek".format(len(valid_sals)), icon="ℹ️")

with skill:
    skill_salary = {}
    for key,val in skills.items():
        count = 0
        sal = 0
        for index, row in valid_sals.iterrows():
            if val in row['Skill']:
                count += 1
                sal += row['per_annum']
        if count > 10:
            skill_salary[val] = sal/count
    skill_salary = {k: v for k, v in sorted(skill_salary.items(), key=lambda item: item[1], reverse=True)}
    skill_salary = pd.DataFrame.from_dict(skill_salary, orient='index', columns=['per_annum'])
    skill_salary = skill_salary.reset_index()
    skill_salary.columns = ['skill', 'per_annum']
    skill_salary['per_annum'] = skill_salary['per_annum'].apply(lambda x: round(x, 2))
    skill_salary = skill_salary.sort_values(by='per_annum', ascending=True)
    # only show the top 20 skills
    skill_salary = skill_salary.tail(30)
    fig = go.Figure(data=[go.Bar(y=skill_salary['skill'], x=skill_salary['per_annum'],
                                orientation='h', text=skill_salary['per_annum'], 
                                textposition='outside', 
                                showlegend=False, 
                                texttemplate='%{text:.3s}'
                                )
                                ])
    # increase the font size and make it black and bold
    fig.update_layout(font=dict(size=12, color='black'))

    # set the title
    fig.update_layout(title_text='Average Salary by Skills')

    # update the x axis range to 50000 to 200000
    fig.update_xaxes(range=[50000, 200000])

    # set size to 1280 x 720
    fig.update_layout(width=1280, height=720)
    # show the chart
    st.plotly_chart(fig, use_container_width=True, sharing='streamlit', config={'displaylogo': False}, theme='streamlit')
    # show some stats
    st.info("Above chart is based on {} jobs scraped from Seek".format(len(valid_sals)), icon="ℹ️")

with lang:
    language_salary = {}
    for key,val in langugages.items():
        count = 0
        sal = 0
        for index, row in valid_sals.iterrows():
            if val in row['Programming']:
                count += 1
                sal += row['per_annum']
        if count > 10:
            language_salary[val] = sal/count
    language_salary = {k: v for k, v in sorted(language_salary.items(), key=lambda item: item[1], reverse=True)}
    language_salary = pd.DataFrame.from_dict(language_salary, orient='index', columns=['per_annum'])
    language_salary = language_salary.reset_index()
    language_salary.columns = ['language', 'per_annum']
    language_salary['per_annum'] = language_salary['per_annum'].apply(lambda x: round(x, 2))
    language_salary = language_salary.sort_values(by='per_annum', ascending=True)
    # show only top 30
    language_salary = language_salary.tail(30)
    fig = go.Figure(data=[go.Bar(y=language_salary['language'], x=language_salary['per_annum'],
                                orientation='h', text=language_salary['per_annum'], 
                                textposition='outside', 
                                showlegend=False, 
                                texttemplate='%{text:.3s}'
                                )
                                ])
    # increase the font size and make it black and bold
    fig.update_layout(font=dict(size=12, color='black'))

    # update the x axis range to 50000 to 200000
    fig.update_xaxes(range=[50000, 200000])

    # set the title
    fig.update_layout(title_text='Average Salary by Programming Language')

    # set size to 1280 x 720
    fig.update_layout(width=1280, height=720)

    # show the chart
    st.plotly_chart(fig, use_container_width=True, sharing='streamlit', config={'displaylogo': False}, theme='streamlit')
    # show some stats
    st.info("Above chart is based on {} jobs scraped from Seek".format(len(valid_sals)), icon="ℹ️")

# add footer
st.markdown("&copy; 2023 All rights reserved. Sujal Dhungana", unsafe_allow_html=True)

