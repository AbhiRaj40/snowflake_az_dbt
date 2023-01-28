import pandas as pd
import yaml
import sqlparse
import os

pd.set_option('display.max_columns', None)

dbt_project_location = r'C:\Users\NagarajaU\PycharmProjects\\'

hana_models_location = r'C:\Users\NagarajaU\PycharmProjects\source_models'

prj_name = input('enter project name : ')

sch_nam = input('enter schema name : ')

schema_location = dbt_project_location + prj_name + r'\models\virtualized\\' + sch_nam

if schema_location[-1] == '\\' or schema_location[-1] == '/':
    schema_location = schema_location[:-1]

if hana_models_location[-1] == '\\' or hana_models_location[-1] == '/':
    hana_models_location = hana_models_location[:-1]


class MyDumper(yaml.Dumper):

    def increase_indent(self, flow=False, indentless=False):
        return super(MyDumper, self).increase_indent(flow, False)


def find_data_type(datatype, length):
    try:

        if 'char' in datatype:

            length = int(float(length))

            return 'varchar(' + str(length) + ')'

        elif ('date' in datatype) or ('time' in datatype):

            return datatype

        elif datatype == 'tinyint':

            return 'number(1,0)'

        elif datatype == 'bigint':

            return 'number'

        elif datatype == 'number':

            if length.isalpha():
                return datatype

            if ',' in length:

                if length[0] == '(' and length[-1] == ')':

                    return str(datatype) + str(length)

                else:

                    return str(datatype) + '(' + str(length) + ')'

            else:

                return str(datatype) + '(' + str(length) + ')'

        elif datatype == 'decimal':

            datatype = 'number'

            if ',' in length:

                if length[0] == '(' and length[-1] == ')':

                    return str(datatype) + str(length)

                else:

                    return str(datatype) + '(' + str(length) + ')'

            else:

                return str(datatype) + '(' + str(length) + ')'

        elif datatype == 'integer':

            if length.isalpha() or length.replace(' ', '') == '':
                return datatype

            datatype = 'number'

            if ',' in length:

                if length[0] == '(' and length[-1] == ')':

                    return str(datatype) + str(length)

                else:

                    return str(datatype) + '(' + str(length) + ')'

            else:

                return str(datatype) + '(' + str(length) + ')'

        else:

            return datatype

    except:

        print('invalid input' + datatype + length)


def generate_delete_query(model_name):
    # print(f'----{model_name}---------')

    df = pd.read_excel('columns_mappings.xlsx', sheet_name='sources')

    df = df.fillna('')

    df = df.applymap(lambda x: str(x).lower())

    df = df.applymap(lambda x: str(x).strip())

    text_sources = ''

    for index, row in df[df['model_name'] == model_name.lower()].iterrows():

        if row['source_schema'].strip() == '' and row['source_tables'].strip() == '':
            continue

        text_sources += '"source (' + "'" + row['source_schema'].strip().upper() + "','" + row[
            'source_tables'].strip().upper() + "'" + ')", '

    return '{{ generate_delete_query ([' + text_sources[:-2] + ']) }}'


def get_dependencies(model_name):
    df = pd.read_excel('columns_mappings.xlsx', sheet_name='sources')

    df = df.fillna('')

    # df = df.applymap(lambda x: str(x).lower())

    df = df.applymap(lambda x: str(x).strip())

    text_sources = []

    for index, row in df[df['model_name'].str.lower() == model_name.lower()].iterrows():

        if row['dependency']:

            text_sources.append((row['hana_source'].strip().replace('"', '').replace('`', ''),
                                 '({{ ref(' + "'" + row['dependency'].strip().upper() + "'" + ') }})'))

        else:

            text_sources.append((row['source_schema'].strip() + '.' + row['source_tables'].strip(),
                                 row['source_schema'].strip().upper() + '_' + row['source_tables'].strip().upper()))

    # print('$$$$$$$$$$$$')

    # print(text_sources)

    return text_sources


def generate_yml(model_name, schema):
    # print('Generate yaml')

    model_name = model_name.split('.')[0].lower()

    df = pd.read_excel('columns_mappings.xlsx')

    df = df.fillna('')

    df2 = pd.read_excel('columns_mappings.xlsx', sheet_name='summary')

    df2 = df2.applymap(lambda x: str(x).lower())

    df2 = df2.applymap(lambda x: str(x).strip())

    summary_text = df2[df2['model'] == model_name]['summary'].iloc[0]

    df = df.applymap(lambda x: str(x).lower())

    df = df.applymap(lambda x: str(x).strip())

    df = df[df['table'] == model_name]

    df['datatype_ext'] = df.apply(lambda row: find_data_type(row['datatype'], row['length']), axis=1)

    '''

    df_pk = df[(df['pk']=='true')|(df['pk']=='yes')|(df['pk'] == 'y')]

    df_date=df[df['datatype'].str.contains("date|time")==True]

    df_integer=df[df['datatype'].str.contains("int|number|decimal")==True]

    df_string=pd.concat([df, df_pk, df_date, df_integer]).drop_duplicates(keep=False)


    column_list=[]

    pk_list=[]

    for i in df_pk.index:

        column_dict={}

        column_dict['name'] = '_'.join(df_pk['column'][i].split())

        column_dict['description'] = ' '.join(df_pk['column'][i].split('_'))

        column_dict['data_type'] = df_pk['datatype_ext'][i]

        if df_pk.shape[0] == 1:

            column_dict['tests'] = ['unique','not_null']

        elif df_pk.shape[0] > 1:

            pk_list.append(column_dict['name'])

        column_list.append(column_dict)

    for i in df_integer.index:

        column_dict={}

        column_dict['name'] = '_'.join(df_integer['column'][i].split())

        column_dict['description'] = ' '.join(df_integer['column'][i].split('_'))

        column_dict['data_type'] = df_integer['datatype_ext'][i]

        column_list.append(column_dict)

    for i in df_string.index:

        column_dict={}

        column_dict['name'] = '_'.join(df_string['column'][i].split())

        column_dict['description'] = ' '.join(df_string['column'][i].split('_'))

        column_dict['data_type'] = df_string['datatype_ext'][i]

        column_list.append(column_dict)

    for i in df_date.index:

        column_dict={}

        column_dict['name'] = '_'.join(df_date['column'][i].split())

        column_dict['description'] = ' '.join(df_date['column'][i].split('_'))

        column_dict['data_type'] = df_date['datatype_ext'][i]

        column_list.append(column_dict)



    '''

    pk_list = []

    column_list = []

    for i in df.index:
        column_dict = {}

        column_dict['name'] = '_'.join(df['column'][i].split()).upper()

        column_dict['description'] = ' '.join(df['column'][i].split('_'))

        column_dict['data_type'] = df['datatype_ext'][i]

        column_list.append(column_dict)

    columns_dict = {'columns': column_list}

    var_expected = "'" + model_name + "_expected'"

    if pk_list:

        tests_list = [{'dbt_utils.unique_combination_of_columns': {'combination_of_columns': pk_list}},

                      {'test_model': {'enabled': '{{ (target.name == "unit_test") | as_bool }}',

                                      "expected_model": "ref('vendor_purchasing_organisation_association_expected')"}}]

    else:

        tests_list = [{'test_model': {'enabled': '{{ (target.name == "unit_test") | as_bool }}',

                                      "expected_model": 'ref(' + var_expected + ')'}}]

    table_dict = {'models': [
        {'name': 'v_' + schema + '__' + model_name, 'description': summary_text, 'columns': column_list,
         'tests': tests_list}]}

    yaml_file = yaml.dump(table_dict, Dumper=MyDumper, sort_keys=False, default_flow_style=False)

    # print(yaml_file)

    query = "select "

    for i in column_list:
        query += i['name'] + '::' + i['data_type'] + ' as ' + i['name'] + ', '

        # query += 'max(length('+i['name'] + ')) as ' + i['name'] +', '

    query = query[:-2].upper()

    query += " FROM " + model_name

    query = sqlparse.format(query, reindent=True)

    # print(query)

    return (yaml_file, query)


def models_extraction(schema_location, hana_models_location):
    schema = schema_location.split("\\")[-1]

    model_list = []

    models_to_convert = []

    for i in os.listdir(schema_location):

        if 'yml' in i:

            model_yml_file = i

        else:

            model_list.append(i.split('.')[0].split('__')[1].lower())

    for i in os.listdir(hana_models_location):

        if i.split('.')[0].lower() not in model_list:
            models_to_convert.append(i)

    return (model_list, models_to_convert, model_yml_file)


def processing(schema_location, hana_models_location):
    model_list, models_to_convert, model_yml_file = models_extraction(schema_location, hana_models_location)

    model_yml_file = schema_location + '\\' + model_yml_file

    yaml_file = '\n\n'

    query = ''

    for i in models_to_convert:

        # print(i)

        model_file_name_full = schema_location + '\\' + 'v_' + sch_nam + '__' + i.lower().split('.')[0] + '.sql'

        model_file_name = i.lower().split('.')[0]

        query = ''

        output = generate_yml(i, sch_nam)

        yaml_file += output[0] + '\n'

        query += output[1] + '\n'

        with open(model_file_name_full, 'w') as f:

            f.write('-- Macro that generates filter query for all source tables to exclude deleted records \n')

            f.write(generate_delete_query(model_file_name) + '\n\n')

            f.write('-- Start of business logic \n')

            f.write(model_file_name + ' as\n\n')

            with open(hana_models_location + '\\' + i, 'r') as fi:
                data_model = fi.read()

                data_model = data_model.replace('"', '').replace('`', '')

                for d in get_dependencies(model_file_name):
                    data_model = data_model.replace(d[0], d[1])

                f.write('(' + data_model + ')')

            f.write('\n\n')

            f.write('-- Enforcing datatype and lengths for target table in snowflake----\n')

            f.write(query)

    # print(yaml_file)

    # print(model_yml_file)

    with open(model_yml_file, 'a') as f:

        f.write(yaml_file.replace('models:', ''))


processing(schema_location, hana_models_location)