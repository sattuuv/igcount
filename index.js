const { Client, GatewayIntentBits, SlashCommandBuilder, EmbedBuilder, AttachmentBuilder, PermissionFlagsBits } = require('discord.js');
const { ApifyClient } = require('apify-client');
const ExcelJS = require('exceljs');
const express = require('express');

// Initialize Express app for Koyeb health checks
const app = express();
const PORT = process.env.PORT || 8000;

// Cache management system
const botCache = {
    channelCounts: new Map(),
    progressSettings: null,
    lastUpdate: null,
    urlCache: new Map(), // Cache extracted URLs
    clear: function() {
        this.channelCounts.clear();
        this.progressSettings = null;
        this.lastUpdate = null;
        this.urlCache.clear();
        console.log('🗑️ Bot cache cleared completely');
    },
    getMemoryUsage: function() {
        return {
            channelCounts: this.channelCounts.size,
            urlCache: this.urlCache.size,
            progressSettings: this.progressSettings ? 'Set' : 'Not set'
        };
    }
};

// Initialize Discord client
let client;

// Health check endpoints for Koyeb
app.get('/', (req, res) => {
    res.status(200).json({
        status: 'Bot is running',
        uptime: process.uptime(),
        memory: Math.round(process.memoryUsage().heapUsed / 1024 / 1024) + 'MB',
        timestamp: new Date().toISOString(),
        bot_ready: client ? client.isReady() : false,
        cache_info: botCache.getMemoryUsage()
    });
});

app.get('/health', (req, res) => {
    const isReady = client ? client.isReady() : false;
    const memUsage = process.memoryUsage();
    res.status(isReady ? 200 : 503).json({
        status: isReady ? 'healthy' : 'unhealthy',
        bot_ready: isReady,
        ws_ping: client ? client.ws.ping : -1,
        uptime: process.uptime(),
        guilds: client ? client.guilds.cache.size : 0,
        memory_usage: {
            rss: Math.round(memUsage.rss / 1024 / 1024) + 'MB',
            heapUsed: Math.round(memUsage.heapUsed / 1024 / 1024) + 'MB',
            heapTotal: Math.round(memUsage.heapTotal / 1024 / 1024) + 'MB'
        },
        cache_info: botCache.getMemoryUsage()
    });
});

// Force garbage collection endpoint
app.post('/gc', (req, res) => {
    try {
        if (global.gc) {
            const before = process.memoryUsage().heapUsed / 1024 / 1024;
            global.gc();
            const after = process.memoryUsage().heapUsed / 1024 / 1024;
            res.json({
                status: 'success',
                memory_before: Math.round(before) + 'MB',
                memory_after: Math.round(after) + 'MB',
                freed: Math.round(before - after) + 'MB'
            });
        } else {
            res.json({ status: 'error', message: 'Garbage collection not available' });
        }
    } catch (error) {
        res.status(500).json({ status: 'error', message: error.message });
    }
});

client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.MessageContent,
        GatewayIntentBits.GuildVoiceStates,
    ],
});

// Initialize Apify client
const apifyClient = new ApifyClient({
    token: process.env.APIFY_TOKEN,
});

// Configuration
const CONFIG = {
    APIFY_TASK_ID: process.env.APIFY_TASK_ID || 'yACwwaUugD0F22xUU',
    VIEWS_CHANNEL_NAME: process.env.VIEWS_CHANNEL_NAME || 'views',
    EXECUTION_CHANNEL_NAME: process.env.EXECUTION_CHANNEL_NAME || 'view-counting-execution',
    PROGRESS_VOICE_CHANNEL_PREFIX: '📊 Progress: ',
    CACHE_DURATION: 5 * 60 * 1000, // 5 minutes
    INTERACTION_TIMEOUT: 14 * 60 * 1000, // 14 minutes (Discord limit is 15)
    MAX_MESSAGES_PER_BATCH: 500, // Limit message fetching
};

// Role-based permission configuration
const ROLE_CONFIG = {
    CAMPAIGN_ROLES: ['Campaign Manager', 'Social Media Manager'],
    ADMIN_ONLY: ['status', 'clearcache'],
    CAMPAIGN_COMMANDS: ['viewscount', 'progressbar', 'updateprogress'],
};

// Permission checker function
const checkPermissions = (interaction, commandName) => {
    const member = interaction.member;
    
    if (interaction.guild.ownerId === member.id) {
        return { allowed: true, reason: 'Server Owner' };
    }
    
    if (member.permissions.has(PermissionFlagsBits.Administrator)) {
        return { allowed: true, reason: 'Administrator' };
    }
    
    if (ROLE_CONFIG.ADMIN_ONLY.includes(commandName)) {
        return { 
            allowed: false, 
            reason: 'This command requires Administrator permissions.' 
        };
    }
    
    if (ROLE_CONFIG.CAMPAIGN_COMMANDS.includes(commandName)) {
        const hasRole = ROLE_CONFIG.CAMPAIGN_ROLES.some(roleName => {
            return member.roles.cache.some(role => role.name === roleName);
        });
        
        if (hasRole) {
            const userRole = ROLE_CONFIG.CAMPAIGN_ROLES.find(roleName => {
                return member.roles.cache.some(role => role.name === roleName);
            });
            return { allowed: true, reason: `Role: ${userRole}` };
        }
        
        return { 
            allowed: false, 
            reason: `You need one of these roles: ${ROLE_CONFIG.CAMPAIGN_ROLES.join(', ')}` 
        };
    }
    
    return { allowed: true, reason: 'Default Access' };
};

// Utility functions
const formatNumber = (num) => {
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return num.toString();
};

const extractInstagramUrls = (messages) => {
    const urlRegex = /https?:\/\/(?:www\.)?instagram\.com\/(?:reel|p)\/[A-Za-z0-9_-]+/g;
    const urls = new Set();

    messages.forEach(message => {
        const matches = message.content.match(urlRegex);
        if (matches) {
            matches.forEach(url => {
                let cleanUrl = url
                    .replace(/\/+$/, '')
                    .replace(/\?.*$/, '')
                    .replace(/#.*$/, '');

                cleanUrl = cleanUrl.replace(/https?:\/\/(?:www\.)?instagram\.com/, 'https://www.instagram.com');

                if (!cleanUrl.endsWith('/')) {
                    cleanUrl += '/';
                }

                urls.add(cleanUrl);
            });
        }
    });

    const urlArray = Array.from(urls);
    console.log(`Extracted URLs (first 5):`, urlArray.slice(0, 5));
    console.log(`Total extracted: ${urlArray.length} unique URLs`);
    return urlArray;
};

const removeDuplicatesByInputUrl = (results) => {
    const seen = new Set();
    const uniqueResults = [];

    results.forEach(item => {
        const normalizedInputUrl = item.inputUrl
            ?.toLowerCase()
            .replace(/\/+$/, '')
            .replace(/\?.*$/, '')
            .replace(/#.*$/, '')
            .replace(/www\./, '');

        const shortCode = item.shortCode?.toLowerCase();
        const videoId = item.id;

        const identifiers = [
            normalizedInputUrl,
            shortCode,
            videoId
        ].filter(Boolean);

        const isDuplicate = identifiers.some(identifier => seen.has(identifier));

        if (!isDuplicate) {
            identifiers.forEach(identifier => seen.add(identifier));
            uniqueResults.push(item);
        } else {
            console.log(`Duplicate detected: ${item.inputUrl} (Short Code: ${item.shortCode})`);
        }
    });

    console.log(`Removed ${results.length - uniqueResults.length} duplicates from Apify results`);
    return uniqueResults;
};

const parseViewsFromChannel = async (channel, useCache = true) => {
    const cacheKey = `views_${channel.id}`;
    
    // Check cache first
    if (useCache && botCache.lastUpdate && 
        (Date.now() - botCache.lastUpdate) < CONFIG.CACHE_DURATION && 
        botCache.channelCounts.size > 0) {
        console.log('📦 Using cached channel counts');
        return new Map(botCache.channelCounts);
    }

    console.log('🔄 Fetching fresh channel counts...');
    const channelCounts = new Map();

    try {
        let allMessages = [];
        let lastMessageId = null;
        let fetchedCount = 0;

        while (fetchedCount < CONFIG.MAX_MESSAGES_PER_BATCH) {
            const options = { limit: Math.min(100, CONFIG.MAX_MESSAGES_PER_BATCH - fetchedCount) };
            if (lastMessageId) {
                options.before = lastMessageId;
            }

            const messages = await channel.messages.fetch(options);
            if (messages.size === 0) break;

            allMessages.push(...Array.from(messages.values()));
            lastMessageId = messages.last().id;
            fetchedCount += messages.size;

            // Rate limiting protection
            if (messages.size === 100) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }

        console.log(`Fetched ${allMessages.length} messages from #${channel.name} (limit: ${CONFIG.MAX_MESSAGES_PER_BATCH})`);

        allMessages.forEach(message => {
            if (message.embeds.length > 0) {
                const embed = message.embeds[0];

                if (embed.title && embed.title.includes('View Count Analysis')) {
                    let channelName = '';
                    let totalViews = 0;

                    if (embed.fields && embed.fields.length > 0) {
                        embed.fields.forEach(field => {
                            if (field.name === '**Channel**') {
                                channelName = field.value.replace(/[<>#]/g, '');
                            }
                            if (field.name === '**Total Views**') {
                                totalViews = parseInt(field.value.replace(/,/g, ''));
                            }
                        });
                    }

                    if (!channelName && embed.description) {
                        const channelMatch = embed.description.match(/\*\*Channel\*\*\s*[<#]*(\w+)[>#]*/);
                        const viewsMatch = embed.description.match(/\*\*Total Views\*\*\s*([\d,]+)/);

                        if (channelMatch) channelName = channelMatch[1];
                        if (viewsMatch) totalViews = parseInt(viewsMatch[1].replace(/,/g, ''));
                    }

                    if (channelName && totalViews > 0) {
                        if (!channelName.startsWith('#')) {
                            channelName = '#' + channelName;
                        }

                        if (!channelCounts.has(channelName)) {
                            channelCounts.set(channelName, totalViews);
                            console.log(`Found latest count for ${channelName}: ${totalViews} views`);
                        }
                    }
                }
            }
        });

        // Update cache
        botCache.channelCounts.clear();
        channelCounts.forEach((views, channel) => {
            botCache.channelCounts.set(channel, views);
        });
        botCache.lastUpdate = Date.now();
        console.log('📦 Cache updated with fresh data');

    } catch (error) {
        console.error('Error parsing views channel:', error);
    }

    return channelCounts;
};

const updateProgressVoiceChannel = async (guild, campaignName, currentViews, targetViews) => {
    try {
        const percentage = Math.min((currentViews / targetViews) * 100, 100);
        const channelName = `${CONFIG.PROGRESS_VOICE_CHANNEL_PREFIX}${campaignName} (${formatNumber(currentViews)}/${formatNumber(targetViews)}) ${percentage.toFixed(1)}%`;

        let progressChannel = guild.channels.cache.find(ch => 
            ch.type === 2 && 
            ch.name.startsWith(CONFIG.PROGRESS_VOICE_CHANNEL_PREFIX)
        );

        if (progressChannel) {
            if (progressChannel.name !== channelName) {
                await progressChannel.setName(channelName);
                console.log(`✅ Updated progress voice channel: ${channelName}`);
            } else {
                console.log(`ℹ️ Progress voice channel unchanged: ${channelName}`);
            }
        } else {
            progressChannel = await guild.channels.create({
                name: channelName,
                type: 2,
                permissionOverwrites: [
                    {
                        id: guild.roles.everyone,
                        deny: ['Connect', 'Speak'],
                    },
                ],
            });
            console.log(`✅ Created new progress voice channel: ${channelName}`);
        }

        // Update cached progress settings
        botCache.progressSettings = {
            campaignName,
            target: targetViews,
            channelId: progressChannel.id
        };

        return progressChannel;
    } catch (error) {
        console.error('❌ Error updating progress voice channel:', error);
        return null;
    }
};

const getProgressSettings = async (guild, useCache = true) => {
    try {
        if (useCache && botCache.progressSettings) {
            console.log('📦 Using cached progress settings');
            return botCache.progressSettings;
        }

        console.log('🔄 Fetching fresh progress settings...');
        
        const progressChannel = guild.channels.cache.find(ch => 
            ch.type === 2 && 
            ch.name.startsWith(CONFIG.PROGRESS_VOICE_CHANNEL_PREFIX)
        );

        if (progressChannel) {
            const channelName = progressChannel.name;
            const match = channelName.match(/📊 Progress: (.+?) \([\d.,KM]+\/([\d.,KM]+)\)/);
            if (match) {
                const campaignName = match[1];
                const targetStr = match[2];

                let target = 0;
                if (targetStr.includes('M')) {
                    target = parseFloat(targetStr) * 1000000;
                } else if (targetStr.includes('K')) {
                    target = parseFloat(targetStr) * 1000;
                } else {
                    target = parseInt(targetStr.replace(/,/g, ''));
                }

                const settings = { 
                    campaignName, 
                    target,
                    channelId: progressChannel.id 
                };

                botCache.progressSettings = settings;
                console.log('📦 Progress settings cached');

                return settings;
            }
        }

        return null;
    } catch (error) {
        console.error('❌ Error getting progress settings:', error);
        return null;
    }
};

const createExcelFile = async (results, channelName) => {
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Instagram Views Data');

    worksheet.columns = [
        { header: 'Video URL', key: 'inputUrl', width: 50 },
        { header: 'Video ID', key: 'id', width: 20 },
        { header: 'Short Code', key: 'shortCode', width: 15 },
        { header: 'Username', key: 'ownerUsername', width: 20 },
        { header: 'Full Name', key: 'ownerFullName', width: 25 },
        { header: 'Video Views', key: 'videoPlayCount', width: 15 },
        { header: 'Likes Count', key: 'likesCount', width: 15 },
        { header: 'Comments Count', key: 'commentsCount', width: 15 },
        { header: 'Timestamp', key: 'timestamp', width: 20 },
        { header: 'Video Duration', key: 'videoDuration', width: 15 }
    ];

    results.forEach(item => {
        worksheet.addRow({
            inputUrl: item.inputUrl,
            id: item.id,
            shortCode: item.shortCode,
            ownerUsername: item.ownerUsername,
            ownerFullName: item.ownerFullName,
            videoPlayCount: item.videoPlayCount || 0,
            likesCount: item.likesCount || 0,
            commentsCount: item.commentsCount || 0,
            timestamp: item.timestamp,
            videoDuration: item.videoDuration || 0
        });
    });

    worksheet.getRow(1).font = { bold: true };
    worksheet.getRow(1).fill = {
        type: 'pattern',
        pattern: 'solid',
        fgColor: { argb: 'FFE0E0E0' }
    };

    const buffer = await workbook.xlsx.writeBuffer();
    return buffer;
};

// Enhanced interaction timeout handler
const withTimeoutProtection = async (interaction, handler) => {
    const startTime = Date.now();
    let timeoutWarning = false;
    
    // Set up a warning at 12 minutes
    const warningTimeout = setTimeout(async () => {
        if (!interaction.replied && !interaction.deferred) return;
        
        timeoutWarning = true;
        try {
            await interaction.editReply({
                content: '⚠️ This operation is taking longer than expected. Please wait...',
                embeds: []
            });
        } catch (error) {
            console.log('Could not send timeout warning:', error.message);
        }
    }, 12 * 60 * 1000); // 12 minutes

    try {
        await handler();
        clearTimeout(warningTimeout);
    } catch (error) {
        clearTimeout(warningTimeout);
        
        const elapsedTime = Date.now() - startTime;
        console.error(`Command failed after ${Math.round(elapsedTime / 1000)}s:`, error);
        
        // Handle different types of Discord errors
        if (error.code === 10062 || error.code === 40060) {
            console.log('Interaction timeout/already acknowledged - sending to execution channel');
            
            // Try to send error to execution channel instead
            const executionChannel = interaction.guild?.channels.cache.find(ch => 
                ch.name === CONFIG.EXECUTION_CHANNEL_NAME
            );
            
            if (executionChannel) {
                await executionChannel.send({
                    content: `❌ Command timeout for ${interaction.user.tag}: ${error.message}`,
                    embeds: [new EmbedBuilder()
                        .setTitle('⏰ Command Timeout')
                        .setDescription(`The \`${interaction.commandName}\` command took too long to execute.`)
                        .addFields(
                            { name: 'User', value: interaction.user.tag, inline: true },
                            { name: 'Duration', value: `${Math.round(elapsedTime / 1000)}s`, inline: true },
                            { name: 'Error', value: error.message, inline: false }
                        )
                        .setColor(0xFF0000)
                        .setTimestamp()
                    ]
                });
            }
        } else {
            // Try to send error response if interaction is still valid
            try {
                if (!interaction.replied && !interaction.deferred) {
                    await interaction.reply('❌ An error occurred while processing your command.');
                } else {
                    await interaction.editReply('❌ An error occurred while processing your command.');
                }
            } catch (replyError) {
                console.error('Could not send error reply:', replyError.message);
            }
        }
        
        throw error;
    }
};

// Updated slash command definitions
const commands = [
    new SlashCommandBuilder()
        .setName('viewscount')
        .setDescription('Count Instagram views for a specific channel')
        .addChannelOption(option =>
            option.setName('channel')
                .setDescription('The channel to analyze')
                .setRequired(true))
        .setDefaultMemberPermissions(null),

    new SlashCommandBuilder()
        .setName('progressbar')
        .setDescription('Create/update progress voice channel')
        .addStringOption(option =>
            option.setName('campaign')
                .setDescription('Campaign name')
                .setRequired(true))
        .addIntegerOption(option =>
            option.setName('target')
                .setDescription('Target view count')
                .setRequired(true))
        .setDefaultMemberPermissions(null),

    new SlashCommandBuilder()
        .setName('updateprogress')
        .setDescription('Manually update progress voice channel with current totals')
        .setDefaultMemberPermissions(null),

    new SlashCommandBuilder()
        .setName('status')
        .setDescription('Check bot status and health')
        .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),

    new SlashCommandBuilder()
        .setName('clearcache')
        .setDescription('Clear bot cache and force refresh of data')
        .setDefaultMemberPermissions(PermissionFlagsBits.Administrator)
];

// Enhanced command handlers
client.on('interactionCreate', async interaction => {
    if (!interaction.isChatInputCommand()) return;

    const { commandName } = interaction;

    try {
        const permissionCheck = checkPermissions(interaction, commandName);
        
        if (!permissionCheck.allowed) {
            const embed = new EmbedBuilder()
                .setTitle('❌ Access Denied')
                .setDescription(permissionCheck.reason)
                .addFields(
                    { 
                        name: '**Required Permissions**', 
                        value: ROLE_CONFIG.CAMPAIGN_COMMANDS.includes(commandName) 
                            ? `**${ROLE_CONFIG.CAMPAIGN_ROLES.join('** or **')}**`
                            : '**Administrator**',
                        inline: false 
                    },
                    { 
                        name: '**Your Roles**', 
                        value: interaction.member.roles.cache
                            .filter(role => role.name !== '@everyone')
                            .map(role => role.name)
                            .join(', ') || 'No special roles',
                        inline: false 
                    }
                )
                .setColor(0xFF0000)
                .setTimestamp();

            await interaction.reply({ embeds: [embed], ephemeral: true });
            return;
        }

        console.log(`✅ Command ${commandName} used by ${interaction.user.tag} (${permissionCheck.reason})`);

        // Execute commands with timeout protection
        if (commandName === 'viewscount') {
            await withTimeoutProtection(interaction, () => handleViewsCount(interaction));
        } else if (commandName === 'progressbar') {
            await withTimeoutProtection(interaction, () => handleProgressBar(interaction));
        } else if (commandName === 'updateprogress') {
            await withTimeoutProtection(interaction, () => handleUpdateProgress(interaction));
        } else if (commandName === 'status') {
            await handleStatus(interaction);
        } else if (commandName === 'clearcache') {
            await handleClearCache(interaction);
        }
    } catch (error) {
        console.error(`❌ Error handling command ${commandName}:`, error);
        // Error handling is done in withTimeoutProtection
    }
});

const handleClearCache = async (interaction) => {
    await interaction.deferReply();

    try {
        const beforeMemory = process.memoryUsage().heapUsed / 1024 / 1024;
        const beforeCacheInfo = botCache.getMemoryUsage();

        // Clear bot cache
        botCache.clear();

        // Force garbage collection if available
        if (global.gc) {
            global.gc();
        }

        const afterMemory = process.memoryUsage().heapUsed / 1024 / 1024;
        const memorySaved = beforeMemory - afterMemory;

        const embed = new EmbedBuilder()
            .setTitle('🗑️ Cache Cleared')
            .setColor(0x00FF00)
            .addFields(
                { name: '**Before - Channel Counts**', value: beforeCacheInfo.channelCounts.toString(), inline: true },
                { name: '**Before - URL Cache**', value: beforeCacheInfo.urlCache.toString(), inline: true },
                { name: '**Progress Settings**', value: beforeCacheInfo.progressSettings, inline: true },
                { name: '**Memory Before**', value: `${beforeMemory.toFixed(1)} MB`, inline: true },
                { name: '**Memory After**', value: `${afterMemory.toFixed(1)} MB`, inline: true },
                { name: '**Memory Freed**', value: `${Math.max(0, memorySaved).toFixed(1)} MB`, inline: true },
                { name: '**Status**', value: 'All cached data cleared. Next operations will fetch fresh data.', inline: false }
            )
            .setTimestamp();

        await interaction.editReply({ embeds: [embed] });
        console.log(`🗑️ Cache cleared by ${interaction.user.tag} - Freed ${memorySaved.toFixed(1)} MB`);

    } catch (error) {
        console.error('Error clearing cache:', error);
        await interaction.editReply('❌ An error occurred while clearing cache.');
    }
};

const handleUpdateProgress = async (interaction) => {
    await interaction.deferReply();

    try {
        const viewsChannel = interaction.guild.channels.cache.find(ch => ch.name === CONFIG.VIEWS_CHANNEL_NAME);

        if (!viewsChannel) {
            await interaction.editReply(`❌ Views channel #${CONFIG.VIEWS_CHANNEL_NAME} not found.`);
            return;
        }

        const progressSettings = await getProgressSettings(interaction.guild, false);

        if (!progressSettings) {
            await interaction.editReply('❌ No progress voice channel found. Use `/progressbar` to create one first.');
            return;
        }

        const channelCounts = await parseViewsFromChannel(viewsChannel, false);
        const totalViews = Array.from(channelCounts.values()).reduce((sum, views) => sum + views, 0);

        const progressChannel = await updateProgressVoiceChannel(
            interaction.guild,
            progressSettings.campaignName,
            totalViews,
            progressSettings.target
        );

        if (progressChannel) {
            const percentage = Math.min((totalViews / progressSettings.target) * 100, 100);

            const embed = new EmbedBuilder()
                .setTitle('🔄 Progress Updated')
                .setColor(0x0099FF)
                .addFields(
                    { 
                        name: `**${progressSettings.campaignName}**`, 
                        value: `${totalViews.toLocaleString()}/${progressSettings.target.toLocaleString()} views (${percentage.toFixed(1)}%)`, 
                        inline: false 
                    },
                    {
                        name: '**Voice Channel**',
                        value: `Updated: ${progressChannel}`,
                        inline: false
                    }
                )
                .setTimestamp();

            if (channelCounts.size > 0) {
                let recentUpdates = '';
                channelCounts.forEach((views, channel) => {
                    recentUpdates += `📈 ${channel}: ${formatNumber(views)} views\n`;
                });
                embed.addFields({ name: '**Current Channel Counts**', value: recentUpdates, inline: false });
            }

            await interaction.editReply({ embeds: [embed] });
        } else {
            await interaction.editReply('❌ Failed to update progress voice channel.');
        }

    } catch (error) {
        console.error('Error updating progress:', error);
        await interaction.editReply('❌ An error occurred while updating progress.');
    }
};

const handleStatus = async (interaction) => {
    await interaction.deferReply();

    const memUsage = process.memoryUsage();
    const cacheInfo = botCache.getMemoryUsage();

    const embed = new EmbedBuilder()
        .setTitle('🤖 Bot Status')
        .setColor(0x00FF00)
        .addFields(
            { name: '🟢 Status', value: 'Online', inline: true },
            { name: '📊 Heap Used', value: `${Math.round(memUsage.heapUsed / 1024 / 1024)} MB`, inline: true },
            { name: '📊 Total Memory', value: `${Math.round(memUsage.rss / 1024 / 1024)} MB`, inline: true },
            { name: '⏱️ Uptime', value: `${Math.floor(process.uptime() / 3600)}h ${Math.floor((process.uptime() % 3600) / 60)}m`, inline: true },
            { name: '📡 Ping', value: `${client.ws.ping}ms`, inline: true },
            { name: '🏠 Guilds', value: client.guilds.cache.size.toString(), inline: true },
            { name: '📦 Channel Cache', value: `${cacheInfo.channelCounts} items`, inline: true },
            { name: '🔗 URL Cache', value: `${cacheInfo.urlCache} items`, inline: true },
            { name: '🎯 Progress Settings', value: cacheInfo.progressSettings, inline: true },
            { name: '🕒 Last Cache Update', value: botCache.lastUpdate ? new Date(botCache.lastUpdate).toLocaleString() : 'Never', inline: false },
            { name: '🔧 Environment', value: 'Koyeb Hosting', inline: false }
        )
        .setTimestamp();

    await interaction.editReply({ embeds: [embed] });
};

const handleViewsCount = async (interaction) => {
    const targetChannel = interaction.options.getChannel('channel');

    await interaction.deferReply();

    try {
        // Send initial status
        await interaction.editReply('🔄 Starting analysis... This may take several minutes.');

        // Check cache for URLs first
        const cacheKey = `urls_${targetChannel.id}`;
        let urls = botCache.urlCache.get(cacheKey);
        
        if (!urls || (Date.now() - (botCache.urlCache.get(cacheKey + '_timestamp') || 0)) > CONFIG.CACHE_DURATION) {
            await interaction.editReply('📥 Fetching messages from channel...');
            
            // Extract URLs from target channel with message limit
            let allMessages = [];
            let lastMessageId = null;
            let fetchedCount = 0;

            while (fetchedCount < CONFIG.MAX_MESSAGES_PER_BATCH) {
                const options = { limit: Math.min(100, CONFIG.MAX_MESSAGES_PER_BATCH - fetchedCount) };
                if (lastMessageId) {
                    options.before = lastMessageId;
                }

                const messages = await targetChannel.messages.fetch(options);
                if (messages.size === 0) break;

                allMessages.push(...Array.from(messages.values()));
                lastMessageId = messages.last().id;
                fetchedCount += messages.size;

                // Update progress and rate limit protection
                if (fetchedCount % 500 === 0) {
                    await interaction.editReply(`📥 Fetched ${fetchedCount} messages... (limit: ${CONFIG.MAX_MESSAGES_PER_BATCH})`);
                }

                if (messages.size === 100) {
                    await new Promise(resolve => setTimeout(resolve, 100));
                }
            }

            urls = extractInstagramUrls(allMessages);
            
            // Cache the URLs
            botCache.urlCache.set(cacheKey, urls);
            botCache.urlCache.set(cacheKey + '_timestamp', Date.now());
            
            console.log(`📦 Cached ${urls.length} URLs for channel ${targetChannel.name}`);
        } else {
            console.log(`📦 Using cached URLs for channel ${targetChannel.name}`);
        }

        if (urls.length === 0) {
            await interaction.editReply('❌ No Instagram URLs found in the specified channel.');
            return;
        }

        await interaction.editReply(`🔍 Found ${urls.length} unique URLs. Processing with Apify...`);

        // Prepare Apify input
        const apifyInput = {
            addParentData: false,
            directUrls: urls,
            enhanceUserSearchWithFacebookPage: false,
            isUserReelFeedURL: true,
            isUserTaggedFeedURL: false,
            resultsLimit: 1,
            resultsType: "posts",
            searchLimit: 1
        };

        console.log(`Sending ${urls.length} URLs to Apify task ${CONFIG.APIFY_TASK_ID}`);

        // Run Apify task
        const run = await apifyClient.task(CONFIG.APIFY_TASK_ID).call(apifyInput);

        await interaction.editReply('⏳ Apify task completed. Fetching results...');

        // Get results
        const { items } = await apifyClient.dataset(run.defaultDatasetId).listItems();

        console.log(`📊 Apify Results Debug:`);
        console.log(`- Dataset ID: ${run.defaultDatasetId}`);
        console.log(`- Items received: ${items.length}`);

        if (items.length === 0) {
            await interaction.editReply('❌ No data retrieved from Apify. Check if URLs are valid Instagram Reels.');
            return;
        }

        // Check if we got error results
        const errorItems = items.filter(item => item.error);
        const validItems = items.filter(item => !item.error && item.videoPlayCount !== undefined);

        console.log(`- Error items: ${errorItems.length}`);
        console.log(`- Valid items: ${validItems.length}`);

        if (validItems.length === 0) {
            await interaction.editReply(`❌ No valid Instagram data found. Possible reasons:
• Videos are private or deleted
• URLs are incorrect format  
• Instagram is blocking requests
• Try with different/newer Reels URLs`);
            return;
        }

        await interaction.editReply('🔄 Processing results...');

        // Remove duplicates
        const uniqueResults = removeDuplicatesByInputUrl(validItems);
        console.log(`- After deduplication: ${uniqueResults.length} items`);

        if (uniqueResults.length === 0) {
            await interaction.editReply('❌ No unique results after deduplication.');
            return;
        }

        // Process results
        const pageStats = new Map();
        let totalViews = 0;
        let totalVideos = 0;
        const failedUrls = [];

        console.log(`🔄 Processing ${uniqueResults.length} unique results...`);

        uniqueResults.forEach((item, index) => {
            if (item.videoPlayCount !== undefined && item.ownerUsername) {
                const username = item.ownerUsername;
                const views = item.videoPlayCount || 0;

                if (!pageStats.has(username)) {
                    pageStats.set(username, {
                        fullName: item.ownerFullName || username,
                        videos: 0,
                        views: 0,
                        topVideos: []
                    });
                }

                const stats = pageStats.get(username);
                stats.videos++;
                stats.views += views;
                stats.topVideos.push({
                    url: `https://instagram.com/reel/${item.shortCode}/`,
                    views: views,
                    shortCode: item.shortCode
                });

                totalViews += views;
                totalVideos++;
            } else {
                if (item.inputUrl) {
                    failedUrls.push(item.inputUrl);
                }
            }
        });

        console.log(`📊 Final Processing Results:`);
        console.log(`- Total Videos: ${totalVideos}`);
        console.log(`- Total Views: ${totalViews}`);
        console.log(`- Total Pages: ${pageStats.size}`);
        console.log(`- Failed URLs: ${failedUrls.length}`);

        if (totalVideos === 0) {
            await interaction.editReply('❌ No valid video data found. Check console logs for details.');
            return;
        }

        await interaction.editReply('📊 Creating report...');

        // Sort top videos globally
        const allVideos = [];
        pageStats.forEach(stats => {
            allVideos.push(...stats.topVideos);
        });
        allVideos.sort((a, b) => b.views - a.views);
        const top10Videos = allVideos.slice(0, 10);

        // Get views channel for progress calculation
        const viewsChannel = interaction.guild.channels.cache.find(ch => ch.name === CONFIG.VIEWS_CHANNEL_NAME);
        let previousCount = 0;

        if (viewsChannel) {
            const previousCounts = await parseViewsFromChannel(viewsChannel);
            const channelRef = `#${targetChannel.name}`;
            previousCount = previousCounts.get(channelRef) || 0;
            console.log(`Previous count for ${channelRef}: ${previousCount}`);
        }

        // Create embed
        const embed = new EmbedBuilder()
            .setTitle('📊 View Count Analysis')
            .setColor(0x0099FF)
            .addFields(
                { name: '**Channel**', value: targetChannel.toString(), inline: true },
                { name: '**Total Videos**', value: totalVideos.toString(), inline: true },
                { name: '**Total Views**', value: totalViews.toLocaleString(), inline: true },
                { name: '**Total Pages**', value: pageStats.size.toString(), inline: true }
            )
            .setTimestamp();

        // Add page breakdown
        let pageBreakdown = '';
        Array.from(pageStats.entries())
            .sort((a, b) => b[1].views - a[1].views)
            .forEach(([username, stats]) => {
                pageBreakdown += `@${username} - ${stats.videos} videos - ${formatNumber(stats.views)} views\n`;
            });

        if (pageBreakdown) {
            embed.addFields({ name: '**📋 Breakdown by Page**', value: pageBreakdown, inline: false });
        }

        // Add top 10 videos
        if (top10Videos.length > 0) {
            let top10Text = '';
            top10Videos.forEach((video, index) => {
                top10Text += `${index + 1}. [${formatNumber(video.views)} views](${video.url})\n`;
            });
            embed.addFields({ name: '**🏆 Top 10 Most Viewed Videos**', value: top10Text, inline: false });
        }

        // Create Excel file
        console.log(`📁 Creating Excel file...`);
        const excelBuffer = await createExcelFile(uniqueResults, targetChannel.name);
        const attachment = new AttachmentBuilder(excelBuffer, { 
            name: `instagram_views_${targetChannel.name}_${new Date().toISOString().split('T')[0]}.xlsx` 
        });

        // Send to views channel if it exists
        if (viewsChannel) {
            console.log(`📤 Sending results to #${CONFIG.VIEWS_CHANNEL_NAME}...`);
            await viewsChannel.send({ embeds: [embed], files: [attachment] });
            console.log(`✅ Results posted to views channel`);
        }

        // Calculate progress update
        const progressDifference = totalViews - previousCount;
        let progressText = '';
        if (previousCount > 0) {
            progressText = `\n\n**Progress Update:** +${formatNumber(progressDifference)} views from previous count`;
        }

        // Auto-update progress voice channel if it exists
        const progressSettings = await getProgressSettings(interaction.guild);
        if (progressSettings && viewsChannel) {
            console.log(`🎯 Updating progress voice channel...`);
            const channelCounts = await parseViewsFromChannel(viewsChannel, false); // Force fresh data
            const newTotalViews = Array.from(channelCounts.values()).reduce((sum, views) => sum + views, 0);

            await updateProgressVoiceChannel(
                interaction.guild,
                progressSettings.campaignName,
                newTotalViews,
                progressSettings.target
            );
            console.log(`✅ Progress voice channel updated with total: ${newTotalViews.toLocaleString()}`);
        }

        console.log(`🎉 Sending final response to user...`);
        await interaction.editReply({ 
            content: `✅ Analysis complete! Results posted to #${CONFIG.VIEWS_CHANNEL_NAME}${progressText}`,
            embeds: [embed], 
            files: [attachment] 
        });

        // Report failed URLs if any
        if (failedUrls.length > 0) {
            const executionChannel = interaction.guild.channels.cache.find(ch => ch.name === CONFIG.EXECUTION_CHANNEL_NAME);
            if (executionChannel) {
                await executionChannel.send(`❌ **Failed URLs (${failedUrls.length}):**\n${failedUrls.slice(0, 10).join('\n')}${failedUrls.length > 10 ? '\n... and more' : ''}`);
            }
        }

        console.log(`✅ Command completed successfully!`);

    } catch (error) {
        console.error('Error in viewscount command:', error);
        throw error; // Let withTimeoutProtection handle it
    }
};

const handleProgressBar = async (interaction) => {
    const campaignName = interaction.options.getString('campaign');
    const targetViews = interaction.options.getInteger('target');

    await interaction.deferReply();

    try {
        const viewsChannel = interaction.guild.channels.cache.find(ch => ch.name === CONFIG.VIEWS_CHANNEL_NAME);

        if (!viewsChannel) {
            await interaction.editReply(`❌ Views channel #${CONFIG.VIEWS_CHANNEL_NAME} not found.`);
            return;
        }

        await interaction.editReply('🔄 Calculating current progress...');

        const channelCounts = await parseViewsFromChannel(viewsChannel);
        const totalViews = Array.from(channelCounts.values()).reduce((sum, views) => sum + views, 0);

        // Create/update progress voice channel
        const progressChannel = await updateProgressVoiceChannel(
            interaction.guild, 
            campaignName, 
            totalViews, 
            targetViews
        );

        if (progressChannel) {
            const percentage = Math.min((totalViews / targetViews) * 100, 100);

            const embed = new EmbedBuilder()
                .setTitle('🎯 Progress Tracker Updated')
                .setColor(0x00FF00)
                .addFields(
                    { 
                        name: `**${campaignName}**`, 
                        value: `${totalViews.toLocaleString()}/${targetViews.toLocaleString()} views (${percentage.toFixed(1)}%)`, 
                        inline: false 
                    },
                    {
                        name: '**Voice Channel**',
                        value: `Progress is now displayed in: ${progressChannel}`,
                        inline: false
                    }
                )
                .setTimestamp();

            if (channelCounts.size > 0) {
                let recentUpdates = '';
                channelCounts.forEach((views, channel) => {
                    recentUpdates += `📈 ${channel}: ${formatNumber(views)} views\n`;
                });
                embed.addFields({ name: '**Recent Channel Counts**', value: recentUpdates, inline: false });
            }

            await interaction.editReply({ embeds: [embed] });
        } else {
            await interaction.editReply('❌ Failed to create/update progress voice channel.');
        }

    } catch (error) {
        console.error('Error in progressbar command:', error);
        await interaction.editReply('❌ An error occurred while updating progress.');
    }
};

// Bot ready event
client.once('ready', async () => {
    console.log(`✅ Bot is ready! Logged in as ${client.user.tag}`);
    console.log(`🏠 Connected to ${client.guilds.cache.size} guilds`);

    // Register slash commands
    try {
        console.log('🔄 Started refreshing application (/) commands.');
        await client.application.commands.set(commands);
        console.log('✅ Successfully reloaded application (/) commands.');
    } catch (error) {
        console.error('❌ Error registering commands:', error);
    }

    // Clean up old cache periodically
    setInterval(() => {
        const now = Date.now();
        const urlCacheKeys = [...botCache.urlCache.keys()];
        
        urlCacheKeys.forEach(key => {
            if (key.endsWith('_timestamp')) {
                const timestamp = botCache.urlCache.get(key);
                if (now - timestamp > CONFIG.CACHE_DURATION * 2) { // Double cache duration for cleanup
                    const baseKey = key.replace('_timestamp', '');
                    botCache.urlCache.delete(key);
                    botCache.urlCache.delete(baseKey);
                    console.log(`🗑️ Cleaned up expired cache for ${baseKey}`);
                }
            }
        });

        // Force garbage collection if available and memory usage is high
        const memUsage = process.memoryUsage();
        if (global.gc && memUsage.heapUsed > 200 * 1024 * 1024) { // 200MB threshold
            global.gc();
            console.log(`🗑️ Performed garbage collection - freed ${(memUsage.heapUsed - process.memoryUsage().heapUsed) / 1024 / 1024}MB`);
        }
    }, 10 * 60 * 1000); // Every 10 minutes
});

// Enhanced error handling
process.on('unhandledRejection', error => {
    console.error('❌ Unhandled promise rejection:', error);
    
    // Try to send to execution channel if it's a Discord-related error
    if (client?.isReady()) {
        client.guilds.cache.forEach(guild => {
            const executionChannel = guild.channels.cache.find(ch => ch.name === CONFIG.EXECUTION_CHANNEL_NAME);
            if (executionChannel) {
                executionChannel.send(`❌ **Unhandled Promise Rejection:**\n\`\`\`${error.message}\`\`\``).catch(() => {});
            }
        });
    }
});

process.on('uncaughtException', error => {
    console.error('❌ Uncaught exception:', error);
    
    // Try to send to execution channel before crashing
    if (client?.isReady()) {
        client.guilds.cache.forEach(guild => {
            const executionChannel = guild.channels.cache.find(ch => ch.name === CONFIG.EXECUTION_CHANNEL_NAME);
            if (executionChannel) {
                executionChannel.send(`❌ **Uncaught Exception (Bot Restarting):**\n\`\`\`${error.message}\`\`\``).catch(() => {});
            }
        });
    }
    
    // Graceful shutdown
    setTimeout(() => {
        process.exit(1);
    }, 1000);
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('📴 Received SIGTERM, shutting down gracefully...');
    botCache.clear();
    client?.destroy();
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log('📴 Received SIGINT, shutting down gracefully...');
    botCache.clear();
    client?.destroy();
    process.exit(0);
});

// Start Express server first
app.listen(PORT, '0.0.0.0', () => {
    console.log(`🌐 Health check server running on port ${PORT}`);
    console.log(`📊 Available endpoints:`);
    console.log(`   GET  / - Basic status`);
    console.log(`   GET  /health - Detailed health check`);
    console.log(`   POST /gc - Force garbage collection`);
});

// Then login to Discord
if (!process.env.DISCORD_TOKEN) {
    console.error('❌ DISCORD_TOKEN environment variable is required');
    process.exit(1);
}

client.login(process.env.DISCORD_TOKEN).catch(error => {
    console.error('❌ Failed to login:', error);
    process.exit(1);
});
