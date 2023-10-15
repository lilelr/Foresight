#!/usr/bin/env python
# coding: utf-8

# In[1]:


# https://pytorch.org/tutorials/beginner/nlp/sequence_models_tutorial.html
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim

torch.manual_seed(1)


# In[2]:


# lele construct the training datasets



def prepare_sequence(seq, to_ix):
    idxs = [to_ix[w] for w in seq]
    return torch.tensor(idxs, dtype=torch.long)


# ix_to_tag = {v:k for k,v in tag_to_ix.items()}
# definition
id_to_nodes = {"Af": "FileScan FactTable", "Ad": "FileScan DimTable", "B": "BroadcastHashJoin", "C": "SortMergeJoin",
               "D": "Union","E":""}
id_to_operators = {"": "", "1": "Filter", "2": "Project", "3": "HashAggregate", "4": "Sort",
                   "5": "TakeOrderedAndProject", "6": "Expand", "7": "Window"}

training_data = [
    ("AdB BC CB BC CE".split(), ["1-2", "2", "2-3-3-1-4", "2-4", "2-5"]),  # TPC-DS Q1
    ("AfD DB BB BC CE".split(), ["1-2", "", "2-3-3", "2-4", "2-4"]),  # TPC-DS Q2
    ("AfB BB BE".split(), ["1", "2", "2-3-3-5"]),  # TPC-DS Q3
    ("AfC CB BC CC CC CC CC CE".split(), ["1-4", "2", "2-3-1-4", "", "2", "2", "2", "2-5"]),  # TPC-DS Q4
    ("AfC CD DB BB BD DE".split(), ["1-4", "2", "", "2", "2-3", "3-5"]),  # TPC-DS Q5
    ("AfB BC CB BC CE".split(), ["1", "2-4", "2", "2-4", "2-3-3-1-5"]),  # TPC-DS Q6
    ("AfB BB BB BB BE".split(), ["1-2", "2", "2", "2", "2-3-5"]),  # TPC-DS Q7
    ("AfC CB BE".split(), ["1-4", "2-3", "2-3-5"]),  # TPC-DS Q8
    ("AfE".split(), ["1-2-3-2-2"]),  # TPC-DS Q9
    ("AdB BC CC CB BB BE".split(), ["1-2", "2-4", "", "1-2", "2", "2-3-5"]),  # TPC-DS Q10
    ("AfC CB BC CC CC CE".split(), ["1-4", "2", "2-3-1-4", "", "2", "2"]),  # TPC-DS Q11
    ("AfB BB BE".split(), ["1", "2", "2-3-4-7-2"]),  # TPC-DS Q12
    ("AfB BB BB BB BB BE".split(), ["1", "2", "2", "2", "2", "2-3"]),  # TPC-DS Q13
    # TPC-DS Q14a
    # TPC-DS Q14b
    ("AfC CC CB BE".split(), ["1-4", "2-4", "2", "2-3"]),  # TPC-DS Q15
    ("AfC CC CB BB BB BE".split(), ["1-4", "2", "", "2", "2", "2-3-3"]),  # TPC-DS Q16
    ("AfC CC CB BB BB BB BE".split(), ["1-4", "2-4", "2", "2", "2", "2", "2-3"]),  # TPC-DS Q17
    ("AfB BC CB BC CB BB BE".split(), ["1-2", "2-4", "2", "2-4", "2", "2", "2-6-3"]),  # TPC-DS Q18
    ("AdB BB BC CB BB BE".split(), ["1-2", "2", "2-4", "2", "2", "2-3"]),  # TPC-DS Q19
    ("AfB BB BE".split(), ["1", "2", "2-3-4-7-2"]),  # TPC-DS Q20
    ("AfB BB BB BE".split(), ["1", "2", "2", "2-3-1"]),  # TPC-DS Q21
    ("AdB BB BB BE".split(), ["1-2", "2", "2", "2-6-3-5"]),  # TPC-DS Q22
    # TPC-DS Q23a
    # TPC-DS Q23b
    # TPC-DS Q24a
    # TPC-DS Q24b
    ("AfC CC CB BB BB BB BB BE".split(), ["1-4", "2-4", "2", "2", "2", "2", "2", "2-3"]),  # TPC-DS Q25
    ("AfB BB BB BB BE".split(), ["1-2", "2", "2", "2", "2-3-5"]),  # TPC-DS Q26
    ("AfB BB BB BB BE".split(), ["1-2", "2", "2", "2", "2-6-3-5"]),  # TPC-DS Q27
    # TPC-DS Q28 有subquery
    ("AfC CC CB BB BB BB BB BE".split(), ["1-4", "2-4", "2", "2", "2", "2", "2", "2-3-5"]),  # TPC-DS Q29
    ("AdB BB BC CC CB BE".split(), ["1-2", "2", "2-3-3-1-4", "2-4", "2", "2-5"]),  # TPC-DS Q30
    ("AdB BB BC CC CC CC CE".split(), ["1", "2", "2-3-4", "2", "", "2", "2-4"]),  # TPC-DS Q31
    ("AdB BC CB BE".split(), ["1-2", "2-3-1-4", "2", "2-3"]),  # TPC-DS Q32
    ("AdB BB BB BD DE".split(), ["1-2", "2", "2", "2-3", "3-5"]),  # TPC-DS Q33
    ("AdB BB BB BC CE".split(), ["1-2", "2", "2", "2-3-1-4", "2-4"]),  # TPC-DS Q34
    ("AdB BC CC CC CB BB BE".split(), ["1-2", "2-4", "", "", "1-2", "2", "2-3-5"]),  # TPC-DS Q35
    # ---------33-----------
    ("AdB BB BB BE".split(), ["1-2", "2", "2", "2-6-3-4-7-2-5"]),  # TPC-DS Q36

    ("AdB BB BB BC CE".split(), ["1-2", "2", "2", "4", "2-3-5"]),  # TPC-DS Q37
    ("AdB BC CC CC CE".split(), ["1-2", "2-4", "2-3-4", "", "2-3"]),  # TPC-DS Q38
    # TPC-DS Q39a
    # TPC-DS Q39b
    ("AfC CB BB BB BE".split(), ["1-4", "2", "2", "2", "2-3"]),  # TPC-DS Q40

    ("AfB BE".split(), ["1-2-3-1-2", "1-3-2"]),  # TPC-DS Q41
    ("AdB BB BE".split(), ["1-2", "2", "2-3-2"]),  # TPC-DS Q42
    ("AdB BB BE".split(), ["1-2", "2", "2-3-2"]),  # TPC-DS Q43
    # TPC-DS Q44
    ("AfB BB BB BB BB BE".split(), ["1", "2", "2", "2", "2", "1-2-3-5"]),  # TPC-DS Q45
    ("AdB BB BB BB BC CB BE".split(), ["1-2", "2", "2", "2", "2-3-4", "2", "2"]),  # TPC-DS Q46
    ("AdB BB BB BC CC CE".split(), ["1", "2", "2", "2-3-4-7-1-7-1-2-4", "2", "2-5"]),  # TPC-DS Q47
    ("AfB BB BB BB BE".split(), ["1", "2", "2", "2", "2-3"]),  # TPC-DS Q48
    ("AfC CB BD DE".split(), ["1-4", "2", "2-3-4-7-4-7-1-2", "3-5"]),  # TPC-DS Q49
    ("AfC CB BB BB BE".split(), ["1-4", "2", "2", "2", "2-3-5"]),  # TPC-DS Q50

    ("AdB BC CE".split(), ["1-2", "2-3-4-7-2-4", "2-4-7-1-5"]),  # TPC-DS Q51
    ("AdB BB BE".split(), ["1-2", "2", "2-3-5"]),  # TPC-DS Q52
    ("AfB BB BE".split(), ["1-2", "2", "2-3-4-7-1-2-5"]),  # TPC-DS Q53
    ("AfD DB BB BC CC CB BB BE".split(), ["1-2", "", "2", "2-4", "2-4", "2", "2", "2-3-3-3-2"]),  # TPC-DS Q54
    ("AdB BB BE".split(), ["1-2", "2", "2-3-5"]),  # TPC-DS Q55
    ("AdB BB BB BD DE".split(), ["1-2", "2", "2", "2-3", "3-5"]),  # TPC-DS Q56
    ("AfB BB BB BC CC CE".split(), ["1", "2", "2", "2-3-4-7-1-7-1-2-4", "2", "2-5"]),  # TPC-DS Q57
    # TPC-DS Q58
    ("AdB BB BB BC CE".split(), ["1", "2-3", "2", "2-4", "2-5"]),  # TPC-DS Q59
    ("AdB BB BB BD DE".split(), ["1-2", "2", "2", "2-3", "3-5"]),  # TPC-DS Q60

    # window
    # id_to_nodes = {"Af":"FileScan FactTable", "Ad":"FileScan DimTable", "B":"BroadcastHashJoin",
    # "C":"SortMergeJoin", "D":"Union"}
    # id_to_operators = {"":"", "1":"Filter", "2":"Project", "3":"HashAggregate",
    # "4":"Sort", "5":"TakeOrderedAndProject","6":"Expand","7":"Window"}
    ("AfB BB BB BB BB BB BB BE".split(), ["1-2", "2", "2", "2", "2", "2", "2-3", "2"]),  # TPC-DS Q61
    ("AfB BB BB BB BE".split(), ["1", "2", "2", "2", "2-3-5"]),  # TPC-DS Q62
    ("AfB BB BB BE".split(), ["1-2", "2", "2", "2-3-4-7-1-2-5"]),  # TPC-DS Q63
    ("AfC CC CB BB BC CB BB BB BB BB BB BB BC CC CB BB BB BC CE".split(),
     ["1-4", "2-3-1-2-4", "2", "2", "2-4", "2", "2", "2", "2", "2", "2", "2", "2-4", "2-4", "2", "2", "2", "2-3-4",
      "2-4"]),  # TPC-DS Q64
    ("AdB BB BB BC CE".split(), ["1-2", "2-3-1", "2", "2-4", "2-2"]),  # TPC-DS Q65
    ("AdB BB BB BB BD DE".split(), ["1", "2", "2", "2", "2-3", "3-2"]),  # TPC-DS Q66
    ("AdB BB BB BE".split(), ["1-2", "2", "2", "2-6-3-4-7-1"]),  # TPC-DS Q67
    ("AdB BB BB BB BC CB BE".split(), ["1-2", "2", "2", "2", "2-3-4", "2", "2-5"]),  # TPC-DS Q68
    ("AdB BC CC CC CB BB BE".split(), ["1-2", "2-4", "", "", "2", "2", "2-3-5"]),  # TPC-DS Q69
    ("AdB BB BC CB BE".split(), ["1", "2", "2-3-4-7-1-2", "", "2-6-3-4-7-2"]),  # TPC-DS Q70

    ("AdB BD DB BB BE".split(), ["1-2", "2", "", "2", "2-3-4"]),  # TPC-DS Q71
    ("AdC CB BB BB BB BB BB BB BB BB BC CE".split(),
     ["1-4", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2-4", "2-3-5"]),  # TPC-DS Q72
    ("AdB BB BB BC CE".split(), ["1-2", "2", "2", "2-3-1-4", "2-4"]),  # TPC-DS Q73
    ("AfC CB BC CC CE".split(), ["1-4", "2", "2-3-1-4", "2", "2-5"]),  # TPC-DS Q74
    ("AfB BB BC CD DC CE".split(), ["1-2", "2", "2-4", "2", "3-3-4", "2-5"]),  # TPC-DS Q75
    ("AfB BB BD DE".split(), ["1", "2", "2", "3-5"]),  # TPC-DS Q76
    ("AdB BB BC CD DE".split(), ["1-2", "2", "2-3-4", "2", "6-3-5"]),  # TPC-DS Q77
    ("AdC CB BC CC CE".split(), ["1-4", "1-2", "2-3-4", "2", "1-2-5"]),  # TPC-DS Q78
    ("AdB BB BB BC CE".split(), ["1-2", "2", "2", "2-3-4", "2-5"]),  # TPC-DS Q79
    ("AfC CB BB BB BD DE".split(), ["1-4", "2", "2", "2", "2-3", "6-3-5"]),  # TPC-DS Q80

    ("AdB BB BC CC CC CE".split(), ["1-2", "2", "2-3-3-1-4", "2-4", "2-4", "2-5"]),  # TPC-DS Q81
    ("AdB BB BC CE".split(), ["1-2", "2", "2-4", "2-3-5"]),  # TPC-DS Q82
    ("AdB BB BB BC CC CE".split(), ["1-2", "2", "2", "2-3-4", "2", "2-5"]),  # TPC-DS Q83
    ("AfB BB BB BB BC CE".split(), ["1-2", "2", "2", "2", "2-4", "2-5"]),  # TPC-DS Q84
    ("AfC CB BB BB BB BB BB BE".split(), ["1-4", "2", "2", "2", "2", "2", "2", "2-3-5"]),  # TPC-DS Q85
    ("AdB BB BE".split(), ["1-2", "2", "6-3-4-7-2-5"]),  # TPC-DS Q86
    ("AdB BC CC CC CE".split(), ["1-2", "2-4", "2-3", "2", "2-3"]),  # TPC-DS Q87
    ("AfB BB BB BB BB BB BB BB BB BB BE".split(), ["1-2", "2", "2", "2-3", "", "", "", "", "", "", ""]),  # TPC-DS Q88
    ("AfB BB BB BE".split(), ["1", "2", "2", "2-3-4-7-1-2-5"]),  # TPC-DS Q89
    ("AfB BB BB BB BE".split(), ["1-2", "2", "2", "2-3", "5"]),  # TPC-DS Q90

    ("AfB BB BC CB BB BB BE".split(), ["1", "2", "2-4", "2", "2", "2", "2-3-4"]),  # TPC-DS Q91
    ("AdB BC CB BE".split(), ["1-2", "2-3-1-4", "2", "2-3"]),  # TPC-DS Q92
    ("AfC CB BE".split(), ["1-4", "2", "2-3-5"]),  # TPC-DS Q93
    ("AfC CC CB BB BB BE".split(), ["1-4", "2", "", "2", "2", "2-3-3"]),  # TPC-DS Q94
    ("AfC CC CC CB BB BB BE".split(), ["1-4", "2", "2", "", "2", "2", "2-3-3"]),  # TPC-DS Q95
    ("AfB BB BB BE".split(), ["1-2", "2", "2", "2-3"]),  # TPC-DS Q96
    ("AdB BC CE".split(), ["1-2", "2-3-4", "2-3"]),  # TPC-DS Q97
    ("AfB BB BE".split(), ["1", "2", "2-3-4-7-2-4-2"]),  # TPC-DS Q98
    ("AfB BB BB BB BE".split(), ["1", "2", "2", "2", "2-3-5"]),  # TPC-DS Q99

]
word_to_ix = {}
# For each words-list (sentence) and tags-list in each tuple of training_data
for sent, tags in training_data:
    for word in sent:
        if word not in word_to_ix:  # word has not been assigned an index yet
            word_to_ix[word] = len(word_to_ix)  # Assign each word with a unique index
# print(word_to_ix)

tag_to_ix = {}
for sent, tags in training_data:
    for tag in tags:
        if tag not in tag_to_ix:  # tag has not been assigned an index yet
            tag_to_ix[tag] = len(tag_to_ix)  # Assign each tag with a unique index
# print(tag_to_ix)

ix_to_tag = {v: k for k, v in tag_to_ix.items()}
# print(ix_to_tag)

# tag_to_ix = {"DET": 0, "NN": 1, "V": 2}  # Assign each tag with a unique index

# These will usually be more like 32 or 64 dimensional.
# We will keep them small, so we can see how the weights change as we train.
EMBEDDING_DIM = 6
HIDDEN_DIM = 6


# In[3]:


class LSTMTagger(nn.Module):

    def __init__(self, embedding_dim, hidden_dim, vocab_size, tagset_size):
        super(LSTMTagger, self).__init__()
        self.hidden_dim = hidden_dim

        self.word_embeddings = nn.Embedding(vocab_size, embedding_dim)

        # The LSTM takes word embeddings as inputs, and outputs hidden states
        # with dimensionality hidden_dim.
        self.lstm = nn.LSTM(embedding_dim, hidden_dim)

        # The linear layer that maps from hidden state space to tag space
        self.hidden2tag = nn.Linear(hidden_dim, tagset_size)

    def forward(self, sentence):
        embeds = self.word_embeddings(sentence)
        lstm_out, _ = self.lstm(embeds.view(len(sentence), 1, -1))
        tag_space = self.hidden2tag(lstm_out.view(len(sentence), -1))
        tag_scores = F.log_softmax(tag_space, dim=1)
        return tag_scores


# In[4]:

class LSTMWrapper:
    def __init__(self):
        self.model = LSTMTagger(EMBEDDING_DIM, HIDDEN_DIM, len(word_to_ix), len(tag_to_ix))
        self.loss_function = nn.NLLLoss()
        self.optimizer = optim.SGD(self.model.parameters(), lr=0.1)

    def train(self):
        # See what the scores are before training
        # Note that element i,j of the output is the score for tag j for word i.
        # Here we don't need to train, so the code is wrapped in torch.no_grad()
        with torch.no_grad():
            inputs = prepare_sequence(training_data[0][0], word_to_ix)
            tag_scores = self.model(inputs)
            # print(tag_scores)

        for epoch in range(300):  # again, normally you would NOT do 300 epochs, it is toy data
            for sentence, tags in training_data:
                # Step 1. Remember that Pytorch accumulates gradients.
                # We need to clear them out before each instance
                self.model.zero_grad()

                # Step 2. Get our inputs ready for the network, that is, turn them into
                # Tensors of word indices.
                sentence_in = prepare_sequence(sentence, word_to_ix)
                targets = prepare_sequence(tags, tag_to_ix)

                # Step 3. Run our forward pass.
                tag_scores = self.model(sentence_in)

                # Step 4. Compute the loss, gradients, and update the parameters by
                #  calling optimizer.step()
                loss = self.loss_function(tag_scores, targets)
                loss.backward()
                self.optimizer.step()

        # See what the scores are after training
        # with torch.no_grad():
        #     inputs = prepare_sequence(training_data[0][0], word_to_ix)
        #     tag_scores = self.model(inputs)
        #
        #     # The sentence is "the dog ate the apple".  i,j corresponds to score for tag j
        #     # for word i. The predicted tag is the maximum scoring tag.
        #     # Here, we can see the predicted sequence below is 0 1 2 0 1
        #     # since 0 is index of the maximum value of row 1,
        #     # 1 is the index of maximum value of row 2, etc.
        #     # Which is DET NOUN VERB DET NOUN, the correct sequence!
        #     # {'1': 0, '2-4': 1, '2': 2, '2-3-3-1-5': 3, '2-3-3-5': 4}
        #     print(tag_scores)
        #     print(torch.max(tag_scores, 1))
        #
        # # In[5]:

        correct_predictions = 0
        total_predictions = 0
        for sentence, tags in training_data:
            # Prepare the sentence for the model
            sentence_in = prepare_sequence(sentence, word_to_ix)

            # Get predicted tag indices
            predicted_tags = torch.argmax(self.model(sentence_in), dim=1)

            # Convert tensor to numpy array
            predicted_tags = predicted_tags.numpy()

            # Convert ground truth tags to indices
            true_tags = [tag_to_ix[tag] for tag in tags]

            # Calculate accuracy for this sentence
            correct_predictions += sum(predicted_tag == true_tag for predicted_tag, true_tag in zip(predicted_tags, true_tags))
            total_predictions += len(tags)

        accuracy = correct_predictions / total_predictions
        print("Accuracy:", accuracy)

        ##模型保存
        torch.save(self.model, "./lstm_predictor.pt")
        print("save model successfully")

    def predict(self,long_chain):
        ##模型加载
        load_model = torch.load("./lstm_predictor.pt")
        ##设置模型进行测试模式
        load_model.eval()
        test_seq = long_chain.split()
        # test_seq="CB BE".split()
        inputs = prepare_sequence(test_seq, word_to_ix)
        tag_scores = load_model(inputs)
        # print(tag_scores)

        ans = torch.max(tag_scores, 1)
        ans_list = ans.indices.numpy().tolist()
        print(ans_list)
        result = []
        for item in ans_list:
            temp_str = ix_to_tag[item]
            temp_list = temp_str.split("-")
            temp_ans = []
            for key in temp_list:
                temp_ans.append(id_to_operators[key])
            print("-".join(temp_ans))
            result.append(temp_ans)
        return result

#
if __name__ == '__main__':
    lstm_wrapper = LSTMWrapper()
    lstm_wrapper.train()
    long_chain="AfB BC CE"
    result = lstm_wrapper.predict(long_chain)
    print(result)



