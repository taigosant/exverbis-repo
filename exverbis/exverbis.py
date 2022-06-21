import json

import networkx as nx
import requests


class Exverbis:

    def __int__(self):
        self.graphs = []

    def get_graphs(self):
        return self.graphs

    def _parse_query(self, query):
        self.graphs = []
        dns = 'https://corenlp.run/'

        # Fetch data from the Core NLP Docker Server
        http_response = requests.post(dns, query)
        http_response = json.loads(http_response.text)

        # Extract the Enhanced Plus Dependencies and the Tokens
        enhanced_plus_plus_dependencies = http_response['sentences'][0]['enhancedPlusPlusDependencies']
        tokens = http_response['sentences'][0]['tokens']

        # Add the ROOT token with index 0
        root_token = {'index': 0, 'word': 'ROOT', 'originalText': 'ROOT', 'lemma': 'ROOT', 'characterOffsetBegin': 0,
                     'characterOffsetEnd': 4, 'pos': 'VB', 'ner': 'O', 'speaker': 'PER0', 'before': '', 'after': ' '}
        tokens.insert(0, root_token)

        # Create the Python Graph
        G = nx.DiGraph()

        # For each token (including the ROOT token), create a Node in the Graph
        for token in tokens:
            G.add_node(token['index'])

        # For each dependency, create an edge between the nodes
        for dependency in enhanced_plus_plus_dependencies:
            G.add_edge(dependency['governor'], dependency['dependent'], dep=dependency['dep'])

        self.graphs.append({
            'query': query,
            'graph': G,
            'tokens': tokens,
            'struct': {},  # Intermediate Structure
            'classifications': '',
            'json': {}
        })

    def _inner_classify(self, word, incidentDependency):
        if 'nmod' in incidentDependency:
            if incidentDependency == 'nmod:after':
                return {'keyword': word, 'op': '>'}
            if incidentDependency == 'nmod:before':
                return {'keyword': word, 'op': '<'}
            if word == 'number':
                return {'keyword': '<neighbor>', 'op': 'COUNT', 'node_type': 'NN'}
            if incidentDependency == 'nmod:per':
                return {'sql': 'GROUP BY', 'keyword': word, 'node_type': 'NN'}
            else:
                return {'keyword': word}
        if incidentDependency == 'dobj':
            if word == 'number':
                return {'keyword': '<neighbor>', 'sql': 'SELECT', 'op': 'COUNT', 'node_type': 'NN'}
            else:
                return {'keyword': word, 'sql': 'SELECT', 'node_type': 'NN'}
        if incidentDependency == 'advmod' and word == 'more':
            return {'op': '>'}
        if incidentDependency == 'nummod':
            return {'number': word}
        if incidentDependency == 'compound':
            return {'compound': [word]}
        if incidentDependency == 'amod':
            if word == 'total':
                return {'sqlModifier': 'SUM', 'sql': 'SELECT', 'node_type': 'NN'}
            if word == 'most':
                return {'sqlModifier': 'MAX', 'sql': 'SELECT', 'node_type': 'NN'}
            if word == 'average':
                return {'sqlModifier': 'AVG', 'sql': 'SELECT', 'node_type': 'NN'}
            if word == 'many':
                return {'op': 'COUNT', 'sql': 'SELECT', 'node_type': 'NN'}
            else:
                return {'modifier': word}
        if incidentDependency == 'conj:and':
            return {'modifier': 'and', 'keyword': word}
        if incidentDependency == 'det' and word == 'each':
            return {'sql': 'GROUP BY', 'node_type': 'NN'}
        if incidentDependency == 'advmod' and word == 'How':
            return {'question': True}
        if incidentDependency == 'nsubj':
            if word == 'number':
                return {'keyword': '<neighbor>', 'op': 'COUNT', 'sql': 'SELECT', 'node_type': 'NN'}
            else:
                return {'keyword': word}
        if incidentDependency == 'acl':
            return {'path': word}
        if incidentDependency == 'nsubjpass':
            return {'keyword': word}

        return None

    def classify(self, word, incidentDependency, index):
        classification = self._inner_classify(word, incidentDependency)
        if classification:
            return {**classification, 'index': index}
        else:
            return None

    def merge(self, dic1, dic2):
        # print('Merging:\n', dic1, '\n', dic2)
        if dic1 is None:
            return dic2
        if dic2 is None:
            return dic1

        # Merge compounds
        compound1 = dic1.pop('compound', None)
        compound2 = dic2.pop('compound', None)

        compound = None
        if compound1:
            compound = compound1
        if compound2:
            if compound:
                compound += compound2
            else:
                compound = compound2

        if compound:
            merged = {**dic1, **dic2, 'compound': compound}
        else:
            merged = {**dic1, **dic2}

        return merged

    def qpa(self, graph, node, incidentDependency, deps=[], path=[], debug=False):
        # Get the useful data
        G = graph['graph']  # Graph
        tokens = graph['tokens']  # Words data

        if (node, incidentDependency) in path:
            return (path, None)
        else:
            path += [(node, incidentDependency)]

            # Used in the QPA recursive context
        classifications = None
        currentNodeWord = tokens[node]['word']

        for neighbor in G[node]:
            currentNeighborDep = G[node][neighbor]['dep']
            neighborWord = tokens[neighbor]['word']

            (path, neighborClassification) = self.qpa(graph, neighbor, currentNeighborDep, deps, path, debug)

            # TODO - Merge classifications
            classifications = self.merge(classifications, neighborClassification)

        # DEBUG
        print('\n[Node:', node, ']: ', currentNodeWord, '  -  ', incidentDependency) if debug else None

        # Classify and merge node
        classifiedNode = self.classify(currentNodeWord, incidentDependency, node)
        print('\t', classifiedNode) if debug else None

        mergedClassifications = self.merge(classifications, classifiedNode)
        print('\t Merged:', mergedClassifications) if debug else None

        # If a keyword was detected, prune
        if mergedClassifications and ('keyword' in mergedClassifications or 'path' in mergedClassifications):
            graph['classifications'].append(mergedClassifications)
            mergedClassifications = None

        return (path, mergedClassifications)

    # The >return me the abstract of "Making database systems usable"< issue
    # -- Which the keyword is reffered to as the quotation marks
    def get_keyword_based_on_quotation_marks(self, graph, index):
        query = graph['query']
        splitted_query = query.split('"')
        index_counter = 0

        for split in splitted_query:
            # Haha
            tokens = split.split(' ')
            for token in tokens:
                if index_counter == index:
                    return tokens
                index_counter += 1

    def extract_keywords(self, graph):
        classifications = graph['classifications']
        keywords = []
        keywords_text = ''

        for sql in classifications:
            if 'keyword' in sql:
                keyword = sql['keyword']

                if 'keyword_within_quotes' in sql:
                    keyword_list = keyword.split(' ')
                    for key in keyword_list:
                        keywords.append(key)
                    keywords_text += '"' + keyword + '"'
                    continue

                if isinstance(keyword, list):
                    keywords.append(keyword)
                    keywords_text += '"' + ' '.join(keyword) + '"'
                    continue

                if 'modifier' in sql and not 'compound' in sql:
                    modifier = sql['modifier']
                    keywords.append(modifier)
                    keywords_text += ' ' + modifier

                if 'compound' in sql:
                    keywords_with_compounds = sql['compound']
                    keywords_with_compounds.append(keyword)
                    keywords.append(keywords_with_compounds)
                    keywords_text += '"' + ' '.join(keywords_with_compounds) + '"'
                else:
                    keywords.append(keyword)
                    keywords_text += ' ' + keyword

        return (keywords, keywords_text)

    def is_in_quotes(self, graph, word):
        query = graph['query']
        splitted_query = query.split('"')
        for i in range(0, len(splitted_query)):
            current_split = splitted_query[i]
            if i % 2 != 0:
                if word in current_split:
                    return current_split
        return None

    ### Add unextracted keywords, such as:
    # return me all the papers, which contain the keyword "Natural Language"
    # "Natural Language" is a unextracted keyword
    def get_unextracted_keywords_classifications(self, graph):
        query = graph['query']
        query.split('"')
        splitted_query = query.split('"')

        unextracted_classifications = []

        for i in range(0, len(splitted_query)):
            current_split = splitted_query[i]
            if i % 2 != 0:
                splitted_words = current_split.split(' ')
                current_classification = {'keyword': splitted_words[0], 'compound': splitted_words[1:],
                                          'keyword_within_quotes': True, 'whole_keyword': current_split}
                unextracted_classifications.append(current_classification)

        return unextracted_classifications

    # Remove the '<neighbor>' keywords
    # -- <neighbor> happens on cases such as: "return me the number of papers"
    # -- { sql: 'SELECT', op: 'COUNT', keyword: '<neighbor>' }, { keyword: 'papers' }
    def qpa_post_processing(self, graph):
        G = graph['graph']
        tokens = graph['tokens']
        classifications = graph['classifications']

        classifications_to_be_removed = []

        print('Classifications:')
        for c in classifications:
            print(c)

        #     print('Post Processing: ', graph['query'])

        for sql in classifications:
            if 'keyword' in sql:
                keyword = sql['keyword']
                keyword_index = sql['index']

                # The: 'return me all the papers in VLDB conference in "University of Michigan"' issue
                #    where University of Michigan is seen as a nmod:of
                compound_word = self.is_in_quotes(graph, keyword)
                #             print('Keyword: ', keyword, 'compound_word: ', compound_word)
                if compound_word and 'compound' not in sql:
                    sql['keyword'] = compound_word
                    sql['keyword_within_quotes'] = True
                elif compound_word:
                    sql['keyword_within_quotes'] = True
                    sql['whole_keyword'] = compound_word

                    # The "number of" issue
                if keyword == '<neighbor>':
                    node = sql['index']
                    for neighbor in G[node]:
                        if G[node][neighbor]['dep'] == 'nmod:of':
                            neighbor_word = tokens[neighbor]['word']
                            sql['keyword'] = neighbor_word
                            classifications_to_be_removed.append({'keyword': neighbor_word, 'index': neighbor})
                            # Remove from classifications the repeated element
                #                         print('Removing: ', sql)
                #                         try:
                #                             classifications.remove( {  'keyword': neighbor_word, 'index': neighbor } )
                #                         except:
                #                             pass

                # The >return me the abstract of "Making database systems usable"< issue
                # -- Which the keyword is reffered to as the quotation marks
                if keyword == '``':
                    compounded_keyword = self.get_keyword_based_on_quotation_marks(graph, keyword_index)
                    sql['keyword'] = compounded_keyword
                    if 'compound' in sql:
                        del sql['compound']

        # Remove classifications because of "number of" issue
        for del_classification in classifications_to_be_removed:
            while True:
                try:
                    classifications.remove(del_classification)
                except Exception as e:
                    break

                    # Remove duplicated classifications
        # ex: return me all the papers, which contain the keyword "Natural Language"
        #  both classifications: {'keyword': 'papers', 'sql': 'SELECT', 'node_type': 'NN', 'index': 5} and { 'keyword': 'papers', 'index': 5 }
        duplicated = {}
        for classification in classifications:
            try:
                duplicated[classification['index']] += [classification]
            except:
                duplicated[classification['index']] = [classification]
        for key in duplicated:
            items = duplicated[key]
            # Index is duplicated
            if len(items) > 1:
                for item in items:
                    if 'sql' not in item:
                        classifications.remove(item)
                        break
        # Remove duplicated classification

        unextracted_classifications = self.get_unextracted_keywords_classifications(graph)

        graph['classifications'] = classifications + unextracted_classifications

        # Extract keywords into graph object
        (keywords, keywords_text) = self.extract_keywords(graph)
        graph['keywords'] = keywords
        graph['keywords_text'] = keywords_text

    def get_select(self, graph):
        # SELECT clauses
        classifications = graph['classifications']
        select_classifications = [clas for clas in classifications if 'sql' in clas and clas['sql'] == 'SELECT']
        # Check if there is a select_classifications
        select_classifications = sorted(select_classifications, key=lambda k: k['index'])
        select = {'items': [], 'text': ''}
        for i in range(0, len(select_classifications)):
            classification = select_classifications[i]
            keyword = classification['keyword'] if 'keyword' in classification else None
            sqlModifier = classification['sqlModifier'] if 'sqlModifier' in classification else None
            op = classification['op'] if 'op' in classification else None

            # There can be more than one DOBJ dependency, so we need to get only the first
            if i == 0:
                select['items'].append({'keyword': keyword, 'op': op, 'sqlModifier': sqlModifier})
                if op:
                    select['text'] = op + '(KM_(' + keyword + '))'
                elif sqlModifier:
                    select['text'] = sqlModifier + '(KM_(' + keyword + '))'
                else:
                    select['text'] = 'KM_(' + keyword + ')'
            else:
                if sqlModifier and op:
                    select['items'].append({'keyword': keyword, 'op': op, 'sqlModifier': sqlModifier})
                    select['text'] += ', ' + op + '(KM_(' + keyword + '))'
                elif op:
                    select['items'].append({'keyword': keyword, 'op': op})
                    select['text'] += ', ' + op + '(KM_(' + keyword + '))'
                elif sqlModifier:
                    select['items'].append({'keyword': keyword, 'sqlModifier': sqlModifier})
                    select['text'] += ', ' + sqlModifier + '(KM_(' + keyword + '))'

        return select

    def get_where(self, graph, Debug=False):
        classifications = graph['classifications']
        where_classifications = [clas for clas in classifications if 'sql' not in clas and 'keyword' in clas]
        where = {'items': [], 'text': []}

        print('where classifications:', where_classifications) if Debug else None

        for clas in where_classifications:
            keyword = clas['keyword']
            leftSide = keyword
            rightSide = keyword
            op = clas['op'] if 'op' in clas else '='

            op = op if 'op' in clas else '='

            if isinstance(keyword, list):
                leftSide = ' '.join(keyword)
                rightSide = ' '.join(keyword)

            if 'number' in clas:
                leftSide = keyword
                rightSide = clas['number']

            if 'keyword_within_quotes' not in clas and 'compound' in clas:
                leftSide = keyword
                rightSide = clas['compound'][0]

            if 'whole_keyword' in clas:
                leftSide = clas['whole_keyword']
                rightSide = leftSide

            where['items'].append({'leftSide': 'KM_(' + leftSide + ')', 'op': '=', 'rightSide': rightSide})
            where['text'].append('KM_(' + leftSide + ') ' + op + ' ' + '\'' + rightSide + '\'')

        return where

    def get_groupby(self, graph):
        classifications = graph['classifications']
        groupby_classifications = [clas for clas in classifications if 'sql' in clas and clas['sql'] == 'GROUP BY']
        groupby = {'items': [], 'text': []}

        if not len(groupby_classifications):
            return None

        first_classification = groupby_classifications[0]
        groupby['items'].append({'keyword': first_classification['keyword']})
        groupby['text'].append('KM_(' + first_classification['keyword'] + ')')
        return groupby

    def run_qpa(self, graph, debug=False):
        graph['classifications'] = []
        graph['keywords'] = []
        graph['keywords_text'] = ''

        self.qpa(graph, 0, 'ROOT', [], [], debug)
        self.qpa_post_processing(graph)
        return graph

    def get_keywords(self, nlq):
        self._parse_query(nlq)
        classification_graph = self.run_qpa(self.graphs[0])
        return {'query': classification_graph['query'], 'keywords': classification_graph['keywords']}



