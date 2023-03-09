import streamlit as st
from data_preparation import default
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


st.set_page_config(page_title="Top Skills For Data Professionals", page_icon="🔨")
st.title("Top Skills For Data Professionals")

with st.sidebar:
    st.header("About")
    st.write("This is a simple app to scrape data from Seek")
    
with st.spinner("Refreshing Data..."):
    if st.button("Refresh Data"):
        default()
    # read the processed data
    df = pd.read_csv('data/processed/seek_scraper_processed_data.csv', na_filter=False)
    st.info("Processed {} jobs".format(len(df)), icon="ℹ️")
left_column, right_column = st.columns(2)

with left_column:
    # job search term selector
    job_selection_list = df['Search Term'].unique().tolist()
    job_selection_list.append("All")
    selection_job_search = st.selectbox('Select a search term', job_selection_list)
    # only show data for selected search term
    if selection_job_search != "All":
        sel_df = df[df['Search Term'] == selection_job_search]
    else:
        sel_df = df

with right_column:
    # Number skill selctor for slider
    count_dict = {"Top 5": 5, "Top 10": 10, "Top 20": 20, "Top 50": 50, "All" : 100}
    selection_skill_number = st.selectbox('Select number of skills to show', list(count_dict.keys()))

# get the count of each skill
skills_count = {}
langugages_count = {}
qualification_count = {}
for index, row in sel_df.iterrows():
    skills = row['Skill'].split(':')
    skills = [skill.strip() for skill in skills]
    langugages = row['Programming'].split(':')
    langugages = [language.strip() for language in langugages]

    qualifications = row['Qualification'].split(':')
    if "Higher Degree" in qualifications:
        if "Higher Degree" in qualification_count:
            qualification_count["Higher Degree"] += 1
        else:
            qualification_count["Higher Degree"] = 1
    elif "Certificate" in qualifications:
        if "Certificate" in qualification_count:
            qualification_count["Certificate"] += 1
        else:
            qualification_count["Certificate"] = 1
    else:
        if "No Degree" in qualification_count:
            qualification_count["No Degree"] += 1
        else:
            qualification_count["No Degree"] = 1
        
    for skill in skills:
        if skill in skills_count:
            skills_count[skill] += 1
        else:
            skills_count[skill] = 1
    for language in langugages:
        if language.strip() in langugages_count:
            langugages_count[language] += 1
        else:
            langugages_count[language] = 1


# create a dataframe from the skills and languages
skills_df = pd.DataFrame.from_dict(skills_count, orient='index', columns=['Count'])
languages_df = pd.DataFrame.from_dict(langugages_count, orient='index', columns=['Count'])
qualification_df = pd.DataFrame.from_dict(qualification_count, orient='index', columns=['Count'])

# drop the row where the skill is empty
skills_df = skills_df.drop('')
# sum of Count column of skills df
total_skills = skills_df['Count'].sum()

# drop the row where the language is empty
languages_df = languages_df.drop('')
# sum of Count column of languages df
total_languages = languages_df['Count'].sum()

# sort the dataframes
skills_df = skills_df.sort_values(by='Count', ascending=False)
languages_df = languages_df.sort_values(by='Count', ascending=False)
qualification_df = qualification_df.sort_values(by='Count', ascending=False)

# convert the count to percentage and round to 2 decimal places
skills_df['Percentage'] = (skills_df['Count'] / len(sel_df)) * 100
skills_df['Percentage'] = skills_df['Percentage'].round(2)
languages_df['Percentage'] = (languages_df['Count'] / len(sel_df)) * 100
languages_df['Percentage'] = languages_df['Percentage'].round(2)
qualification_df['Percentage'] = (qualification_df['Count'] / len(sel_df)) * 100
qualification_df['Percentage'] = qualification_df['Percentage'].round(2)


# limit the number of skills and languages to show based on the slider
if selection_skill_number != "All":
    skills_df = skills_df.head(count_dict[selection_skill_number])
    languages_df = languages_df.head(count_dict[selection_skill_number])
    qualification_df = qualification_df.head(count_dict[selection_skill_number])



skill_selection_dict = {"Skills": skills_df, "Languages": languages_df, "Qualifications": qualification_df}
st.write("###  Select a chart to show")
selection = st.radio("select a chart", skill_selection_dict.keys(), horizontal=True, label_visibility='hidden')
# show the skills, languages or qualifications
sorted_df  = skill_selection_dict[selection].sort_values(by='Count', ascending=True)

# produce same graph using go
fig = go.Figure(data=[go.Bar(y=sorted_df.index, x=sorted_df['Percentage'], 
                             orientation='h', text=sorted_df['Percentage'], 
                             textposition='outside', 
                             showlegend=False, 
                             texttemplate='%{text:.2s}%', 
                             marker_color=sorted_df['Count'], 
                             hovertemplate='Skill: %{y}<br>Percentage: %{x:.2s}% <br> Skills Count: %{marker.color:.0f} <extra></extra>')])

# reduce graph margins
fig.update_layout(margin=dict(l=0, r=0, t=30, b=0))

# increase the font size and make it black and bold
fig.update_layout(font=dict(size=12, color='black'))

# increase the font size for y axis
fig.update_yaxes(title_font=dict(size=14))

# set the title
fig.update_layout(title_text=f"{selection_skill_number} {selection} for {selection_job_search}")
# show the chart
st.plotly_chart(fig, use_container_width=True, sharing='streamlit', config={'displaylogo': False}, theme='streamlit')

# show some stats
st.info("Above chart is based on {} jobs scraped from Seek".format(len(sel_df)), icon="ℹ️")


# add footer
st.markdown("&copy; 2023 All rights reserved. Sujal Dhungana", unsafe_allow_html=True)

