import streamlit as st
from data_preparation import default
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


st.set_page_config(page_title="Top Skills For Data Professionals", page_icon="🔨", layout="wide")
st.title("Top Skills For Data Professionals")

with st.sidebar:
    st.header("About")
    st.markdown(""" ### This is a simple app to scrape data from Seek.
    * you can use it to find the top skills for data professionals 
    * you can also use it to find the salaries for those roles
    Hope this helps you in your job search! 
    """)
    
with st.spinner("Refreshing Data..."):
    if st.button("Refresh Data"):
        default()
    # read the processed data
    df = pd.read_csv('data/processed/seek_scraper_processed_data.csv', na_filter=False)
    st.info("Processed {} jobs".format(len(df)), icon="ℹ️")

left_column, mid_column, right_column = st.columns(3)

with left_column:
    # job search term selector
    job_selection_list = df['searchKeywords'].unique().tolist()
    # add All to the beginning of the list
    job_selection_list.insert(0, "All")
    selection_job_search = st.selectbox('Select a search term', job_selection_list, index=0)
    # only show data for selected search term
    if selection_job_search != "All":
        sel_df = df[df['searchKeywords'] == selection_job_search]
    else:
        sel_df = df
with mid_column:
    # job location selector
    location_selection_list = sel_df['jobLocation'].unique().tolist()
    # add All to the beginning of the list
    location_selection_list.insert(0, "All")
    selection_job_location = st.selectbox('Select a location', location_selection_list, index=0)
    # only show data for selected location
    if selection_job_location != "All":
        sel_df = sel_df[sel_df['jobLocation'] == selection_job_location]

with right_column:
    # Number skill selctor for slider
    count_dict = {"Top 5": 5, "Top 10": 10, "Top 20": 20, "Top 50": 50, "All" : 100}
    selection_skill_number = st.selectbox('Select number of skills to show', list(count_dict.keys()), index=4)

l2, r2 = st.columns(2)
with l2:
    # industry selector
    industry_selection_list = sel_df['jobClassification'].unique().tolist()
    # add All to the beginning of the list
    industry_selection_list.insert(0, "All")
    selection_industry = st.selectbox('Select an industry', industry_selection_list)
    # only show data for selected industry
    if selection_industry != "All":
        sel_df = sel_df[sel_df['jobClassification'] == selection_industry]

with r2:
    # job type selector
    job_type_selection_list = sel_df['jobWorkType'].unique().tolist()
    # add All to the beginning of the list
    job_type_selection_list.insert(0, "All")
    selection_job_type = st.selectbox('Select a job type', job_type_selection_list, index=0)
    # only show data for selected job type
    if selection_job_type != "All":
        sel_df = sel_df[sel_df['jobWorkType'] == selection_job_type]

# with r2:
#     # salary range selector
#     salary_min = min(int(df['per_annum'].min()), 500000)
#     salary_max = min(int(df['per_annum'].max()), 500000)
#     options = [x for x in range(salary_min, salary_max, 10000)]
#     sel_min, sel_max = st.select_slider("Select salary range", options=options, value=(salary_min, salary_max))
#     sel_min = int(sel_min)
#     sel_max = int(sel_max)
#     # only show data for selected salary range
#     sel_df = sel_df[sel_df['per_annum'] >= sel_min]
#     sel_df = sel_df[sel_df['per_annum'] <= sel_max]

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

skills, language, qualification,salary =  st.tabs(["Skills", "Languages", "Qualifications", "Salary"])
with skills:
    # show the skills, languages or qualifications
    sorted_df  = skills_df.sort_values(by='Count', ascending=True)
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

    # show the chart
    st.plotly_chart(fig, use_container_width=True, sharing='streamlit', config={'displaylogo': False}, theme='streamlit')
    # show some stats
    st.info("Above chart is based on {} jobs scraped from Seek".format(len(sel_df)), icon="ℹ️")

with language:
    # show the skills, languages or qualifications
    sorted_df  = languages_df.sort_values(by='Count', ascending=True)
    # produce same graph using go
    fig = go.Figure(data=[go.Bar(y=sorted_df.index, x=sorted_df['Percentage'], 
                                orientation='h', text=sorted_df['Percentage'], 
                                textposition='outside', 
                                showlegend=False, 
                                texttemplate='%{text:.2s}%', 
                                marker_color=sorted_df['Count'], 
                                hovertemplate='Language: %{y}<br>Percentage: %{x:.2s}% <br> Language Count: %{marker.color:.0f} <extra></extra>')])

    # reduce graph margins
    fig.update_layout(margin=dict(l=0, r=0, t=30, b=0))

    # increase the font size and make it black and bold
    fig.update_layout(font=dict(size=12, color='black'))

    # increase the font size for y axis
    fig.update_yaxes(title_font=dict(size=14))

    # show the chart
    st.plotly_chart(fig, use_container_width=True, sharing='streamlit', config={'displaylogo': False}, theme='streamlit')
    # show some stats
    st.info("Above chart is based on {} jobs scraped from Seek".format(len(sel_df)), icon="ℹ️")

with qualification:
    # show the skills, languages or qualifications
    sorted_df  = qualification_df.sort_values(by='Count', ascending=True)
    # produce same graph using go
    fig = go.Figure(data=[go.Bar(y=sorted_df.index, x=sorted_df['Percentage'], 
                                orientation='h', text=sorted_df['Percentage'], 
                                textposition='outside', 
                                showlegend=False, 
                                texttemplate='%{text:.2s}%', 
                                marker_color=sorted_df['Count'], 
                                hovertemplate='Qualification: %{y}<br>Percentage: %{x:.2s}% <br> Qualification Count: %{marker.color:.0f} <extra></extra>')])

    # reduce graph margins
    fig.update_layout(margin=dict(l=0, r=0, t=30, b=0))

    # increase the font size and make it black and bold
    fig.update_layout(font=dict(size=12, color='black'))

    # increase the font size for y axis
    fig.update_yaxes(title_font=dict(size=14))

    # show the chart
    st.plotly_chart(fig, use_container_width=True, sharing='streamlit', config={'displaylogo': False}, theme='streamlit')
    # show some stats
    st.info("Above chart is based on {} jobs scraped from Seek".format(len(sel_df)), icon="ℹ️")
with salary:
    topby_dict = {'Top by Salary': 'per_annum', 'Top by Count': 'count'}
    selection_type = st.radio("Select a view", list(topby_dict.keys()), horizontal=True)
    san_sal_df = sel_df[(sel_df['per_annum'] > 0) & (sel_df['per_annum'] < 600000)]
    # sanitise job title only keep the first 30 letters
    san_sal_df['jobTitle'] = san_sal_df['jobTitle'].apply(lambda x: x[:35])
    # aggregated stats
    agg_df = san_sal_df.groupby('jobTitle').agg({'per_annum': 'mean', 
                                            'per_hour': 'mean',
                                                'per_day': 'mean',
                                                'jobTitle': 'count' }).rename(columns={'jobTitle': 'count'}).reset_index()

    # make it into a dataframe
    agg_df = pd.DataFrame(agg_df)
    if selection_type == 'Top by Salary':
        agg_df = agg_df.sort_values(by='per_annum', ascending=False)
        # limit the number of rows based on selection
        agg_df = agg_df.head(count_dict[selection_skill_number])
    else:
        agg_df = agg_df.sort_values(by='count', ascending=False)
        # limit the number of rows based on selection
        agg_df = agg_df.head(count_dict[selection_skill_number])
        
    # # produce same graph using go
    fig = go.Figure(data=[go.Bar(y=agg_df['jobTitle'], x=agg_df['per_annum'], 
                                orientation='h', text=agg_df['per_annum'], 
                                textposition='outside', 
                                showlegend=False, 
                                texttemplate='%{text:.2s}', 
                                marker_color=agg_df[topby_dict[selection_type]]
                                )])
    fig.update_layout(yaxis=dict(autorange="reversed"))
    # reduce graph margins
    fig.update_layout(margin=dict(l=0, r=0, t=30, b=0))

    # increase the font size and make it black and bold
    fig.update_layout(font=dict(size=12, color='black'))

    # increase the font size for y axis
    fig.update_yaxes(title_font=dict(size=14))


    # show the chart
    st.plotly_chart(fig, use_container_width=True, sharing='streamlit', config={'displaylogo': False}, theme='streamlit')

    # show some stats
    st.info("Above chart is based on {} jobs scraped from Seek".format(len(san_sal_df)), icon="ℹ️")


# add footer
st.markdown("&copy; 2023 All rights reserved. Sujal Dhungana", unsafe_allow_html=True)

