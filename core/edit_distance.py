# 编辑距离问题
def edit_dist(a, b):
    m, n = len(a) + 1, len(b) + 1

    d = [[0] * n for i in range(m)]

    d[0][0] = 0
    for i in range(1, m):
        d[i][0] = d[i - 1][0] + 1

    for j in range(1, n):
        d[0][j] = d[0][j - 1] + 1

    temp = 0

    for i in range(1, m):
        for j in range(1, n):
            if a[i - 1] == b[j - 1]:
                temp = 0
            else:
                temp = 1

            d[i][j] = min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + temp)

    # # 输出d[i][j]矩阵
    # for i in range(m):
    #     print(d[i])

    return d[m - 1][n - 1]


def best_fit(current_filter_name, tb1_name, tb1_fields, tb2_name, tb2_fields):
    best_dist = 10000
    min_item = ""
    final_tb_name = tb1_name
    for item in tb1_fields:
        dist_now = edit_dist(current_filter_name, item)
        if dist_now < best_dist:
            best_dist = dist_now
            min_item = item
            final_tb_name = tb1_name

    for item in tb2_fields:
        dist_now = edit_dist(current_filter_name, item)
        if dist_now < best_dist:
            best_dist = dist_now
            min_item = item
            final_tb_name = tb2_name

    return final_tb_name, min_item


# if __name__ == '__main__':
#     ed = edit_dist("coffee", "coffe")
#     print('编辑距离为：', ed)
