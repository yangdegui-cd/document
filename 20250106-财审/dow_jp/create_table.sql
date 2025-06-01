-- 在Mysql客户端执行.  Trino客户端不支持主键自增和索引.
-- 月统计
CREATE TABLE IF NOT EXISTS zhanguo_jp_shenji_stat_base.audit_month_diamond (
    id INT(11) NOT NULL AUTO_INCREMENT,
    month VARCHAR(10) NOT NULL,
    channel VARCHAR(255) NOT NULL,
    money DECIMAL(18, 2) NOT NULL,
    diamond_value DECIMAL(18, 2),
    once_item_income DECIMAL(18, 2),
    diamond_add DECIMAL(18, 2),
    diamond_cost DECIMAL(18, 2),
    diamond_cost_once DECIMAL(18, 2),
    diamond_cost_once_rio VARCHAR(10),
    diamond_cost_forever DECIMAL(18, 2),
    diamond_cost_forever_rio VARCHAR(10),
    diamond_cost_play DECIMAL(18, 2),
    diamond_cost_play_rio VARCHAR(10),
    wucha DECIMAL(18, 2),
    wucha_rio VARCHAR(10),
    PRIMARY KEY (id),
    KEY idx_auditmonthdiamond_datemonth (month),
    KEY idx_auditmonthdiamond_channel (channel)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 一次性道具
CREATE TABLE IF NOT EXISTS zhanguo_jp_shenji_stat_base.audit_month_once_item (
    id INT(11) NOT NULL AUTO_INCREMENT,
    month INT(11) NOT NULL,
    channel VARCHAR(16) NOT NULL,
    itemid VARCHAR(16) NOT NULL,
    itemname VARCHAR(16),
    itemid_add DOUBLE DEFAULT NULL,
    itemid_cost DOUBLE DEFAULT NULL,
    itemid_cost_diamond DOUBLE DEFAULT NULL,
    itemid_value DOUBLE DEFAULT NULL,
    PRIMARY KEY (id),
    KEY idx_auditmonthonce_datemonth (month),
    KEY idx_auditmonthonce_channel (channel),
    KEY idx_auditmonthonce_itemid (itemid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 永久性道具
CREATE TABLE IF NOT EXISTS zhanguo_jp_shenji_stat_base.audit_month_forever_item (
    id INT(11) NOT NULL AUTO_INCREMENT,
    month INT(11) NOT NULL,
    channel VARCHAR(16) NOT NULL,
    itemid VARCHAR(16) NOT NULL,
    itemname VARCHAR(16),
    itemid_add_num INT DEFAULT NULL,
    itemid_cost_diamond DOUBLE DEFAULT NULL,
    PRIMARY KEY (id),
    KEY idx_auditmonthforver_datemonth (month),
    KEY idx_auditmonthforver_channel (channel),
    KEY idx_auditmonthforver_itemid (itemid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 玩法消耗
CREATE TABLE IF NOT EXISTS zhanguo_jp_shenji_stat_base.audit_month_play (
    id INT(11) NOT NULL AUTO_INCREMENT,
    month INT(11) NOT NULL,
    channel VARCHAR(16) NOT NULL,
    reason INT,
    core_cost DOUBLE,
    PRIMARY KEY (id),
    KEY idx_auditmonthplay_datemonth (month),
    KEY idx_auditmonthplay_channel (channel)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;