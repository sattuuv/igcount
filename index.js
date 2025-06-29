const { Client, GatewayIntentBits, SlashCommandBuilder, EmbedBuilder, AttachmentBuilder, PermissionFlagsBits } = require('discord.js');
const { ApifyClient } = require('apify-client');
const ExcelJS = require('exceljs');

// Initialize Discord client optimized for Koyeb
const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.MessageContent,
    ],
    partials: [],
    presence: {
        status: 'online',
        activities: [{
            name: 'Instagram Views',
            type: 3 // Watching
        }]
    }
});

// Initialize Apify client with error handling
let apifyClient;
try {
    apifyClient = new ApifyClient({
        token: process.env.APIFY_TOKEN,
    });
} catch (error) {
    console.error('❌ Failed to initialize Apify client:', error);
}

// Configuration optimized for Koyeb
const CONFIG = {
    APIFY_TASK_ID: process.env.APIFY_TASK_ID || 'yACwwaUugD0F22xUU',
    VIEWS_CHANNEL_NAME: process.env.VIEWS_CHANNEL_NAME || 'views',
    EXECUTION_CHANNEL_NAME: process.env.EXECUTION_CHANNEL_NAME || 'view-counting-execution',
    PROGRESS_VOICE_CHANNEL_PREFIX: '📊 Progress: ',
    // Koyeb-specific optimizations
    MAX_CONCURRENT_REQUESTS: 10,
    REQUEST_DELAY: 100,
    MEMORY_CLEANUP_INTERVAL: 300000, // 5 minutes
    MAX_MESSAGES_FETCH: 15000, // Increased for Koyeb's better resources
    BATCH_SIZE: 200
};

// Health check endpoint for Koyeb
const express = require('express');
const app = express();
const PORT = process.env.PORT || 8000;

app.get('/', (req, res) => {
    res.json({
        status: 'Bot is running',
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        guilds: client.guilds.cache.size,
        timestamp: new Date().toISOString()
    });
});

app.get('/health', (req, res) => {
    const isReady = client.isReady();
    res.status(isReady ? 200 : 503).json({
        status: isReady ? 'healthy' : 'unhealthy',
        bot_ready: isReady,
        ws_ping: client.ws.ping,
        uptime: process.uptime()
    });
});

// Start Express server for Koyeb health checks
app.listen(PORT, '0.0.0.0', () => {
    console.log(`🌐 Health check server running on port ${PORT}`);
});

// Utility functions
const formatNumber = (num) => {
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return num.toString();
};

// Optimized message fetching for Koyeb
const fetchAllMessages = async (channel, maxMessages = CONFIG.MAX_MESSAGES_FETCH) => {
    const messages = [];
    let lastMessageId = null;
    let fetchedCount = 0;

    try {
        while (fetchedCount < maxMessages) {
            const options = { limit: Math.min(100, maxMessages - fetchedCount) };
            if (lastMessageId) {
                options.before = lastMessageId;
            }

            const batch = await channel.messages.fetch(options);
            if (batch.size === 0) break;

            const batchArray = Array.from(batch.values());
            messages.push(...batchArray);
            fetchedCount += batchArray.length;
            lastMessageId = batch.last().id;

            // Smaller delay for Koyeb's better performance
            if (batch.size === 100) {
                await new Promise(resolve => setTimeout(resolve, CONFIG.REQUEST_DELAY));
            }

            // Memory management
            if (fetchedCount % 2000 === 0) {
                console.log(`📨 Fetched ${fetchedCount} messages...`);
                if (global.gc) global.gc();
            }
        }

        console.log(`📨 Total fetched: ${messages.length} messages from #${channel.name}`);
        return messages;
    } catch (error) {
        console.error('❌ Error fetching messages:', error);
        return messages;
    }
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
    console.log(`🔗 Extracted ${urlArray.length} unique URLs`);
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

        const identifiers = [normalizedInputUrl, shortCode, videoId].filter(Boolean);
        const isDuplicate = identifiers.some(identifier => seen.has(identifier));

        if (!isDuplicate) {
            identifiers.forEach(identifier => seen.add(identifier));
            uniqueResults.push(item);
        }
    });

    console.log(`🔄 Removed ${results.length - uniqueResults.length} duplicates`);
    return uniqueResults;
};

const parseViewsFromChannel = async (channel) => {
    const channelCounts = new Map();

    try {
        const allMessages = await fetchAllMessages(channel, 8000);

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
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error('❌ Error parsing views channel:', error);
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
            await progressChannel.setName(channelName);
            console.log(`📊 Updated progress voice channel`);
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
            console.log(`📊 Created new progress voice channel`);
        }

        return progressChannel;
    } catch (error) {
        console.error('❌ Error updating progress voice channel:', error);
        return null;
    }
};

const getProgressSettings = async (guild) => {
    try {
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

                return { campaignName, target };
            }
        }

        return null;
    } catch (error) {
        console.error('❌ Error getting progress settings:', error);
        return null;
    }
};

// Optimized Excel creation for Koyeb
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

    // Process in batches for memory efficiency
    const batchSize = CONFIG.BATCH_SIZE;
    for (let i = 0; i < results.length; i += batchSize) {
        const batch = results.slice(i, i + batchSize);
        batch.forEach(item => {
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
        
        // Memory management for large files
        if (i % (batchSize * 5) === 0 && global.gc) {
            global.gc();
        }
    }

    worksheet.getRow(1).font = { bold: true };
    worksheet.getRow(1).fill = {
        type: 'pattern',
        pattern: 'solid',
        fgColor: { argb: 'FFE0E0E0' }
    };

    const buffer = await workbook.xlsx.writeBuffer();
    return buffer;
};

// Slash command definitions
const commands = [
    new SlashCommandBuilder()
        .setName('viewscount')
        .setDescription('Count Instagram views for a specific channel')
        .addChannelOption(option =>
            option.setName('channel')
                .setDescription('The channel to analyze')
                .setRequired(true))
        .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),

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
        .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),

    new SlashCommandBuilder()
        .setName('status')
        .setDescription('Check bot status and health')
        .setDefaultMemberPermissions(PermissionFlagsBits.Administrator)
];

// Command handlers with Koyeb optimizations
client.on('interactionCreate', async interaction => {
    if (!interaction.isChatInputCommand()) return;

    const { commandName } = interaction;

    try {
        if (commandName === 'viewscount') {
            await handleViewsCount(interaction);
        } else if (commandName === 'progressbar') {
            await handleProgressBar(interaction);
        } else if (commandName === 'status') {
            await handleStatus(interaction);
        }
    } catch (error) {
        console.error(`❌ Error handling command ${commandName}:`, error);
        if (!interaction.replied && !interaction.deferred) {
            await interaction.reply('❌ An error occurred while processing your command.');
        } else {
            await interaction.editReply('❌ An error occurred while processing your command.');
        }
    }
});

// Enhanced status command for Koyeb monitoring
const handleStatus = async (interaction) => {
    await interaction.deferReply();

    const memUsage = process.memoryUsage();
    const uptime = process.uptime();

    const embed = new EmbedBuilder()
        .setTitle('🤖 Bot Status - Koyeb')
        .setColor(0x00FF00)
        .addFields(
            { name: '🟢 Status', value: 'Online & Healthy', inline: true },
            { name: '📊 Memory', value: `${Math.round(memUsage.heapUsed / 1024 / 1024)}MB / ${Math.round(memUsage.heapTotal / 1024 / 1024)}MB`, inline: true },
            { name: '⏱️ Uptime', value: `${Math.floor(uptime / 3600)}h ${Math.floor((uptime % 3600) / 60)}m`, inline: true },
            { name: '🔧 Platform', value: 'Koyeb Cloud', inline: true },
            { name: '📡 WebSocket', value: `${client.ws.ping}ms`, inline: true },
            { name: '🏠 Servers', value: client.guilds.cache.size.toString(), inline: true },
            { name: '👥 Users', value: client.users.cache.size.toString(), inline: true },
            { name: '🌐 Health Endpoint', value: `Port ${PORT}`, inline: true },
            { name: '📈 Node.js', value: process.version, inline: true }
        )
        .setTimestamp()
        .setFooter({ text: 'Hosted on Koyeb' });

    await interaction.editReply({ embeds: [embed] });
};

const handleViewsCount = async (interaction) => {
    const targetChannel = interaction.options.getChannel('channel');

    await interaction.deferReply();

    try {
        console.log(`🔍 Starting analysis for channel: ${targetChannel.name}`);
        
        const allMessages = await fetchAllMessages(targetChannel);
        const urls = extractInstagramUrls(allMessages);

        if (urls.length === 0) {
            await interaction.editReply('❌ No Instagram URLs found in the specified channel.');
            return;
        }

        await interaction.editReply(`🔍 Found ${urls.length} unique URLs. Processing with Apify...`);

        if (!apifyClient) {
            await interaction.editReply('❌ Apify client not available. Check APIFY_TOKEN environment variable.');
            return;
        }

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

        console.log(`🚀 Processing ${urls.length} URLs with Apify task: ${CONFIG.APIFY_TASK_ID}`);

        const run = await apifyClient.task(CONFIG.APIFY_TASK_ID).call(apifyInput);
        await interaction.editReply('⏳ Apify task completed. Fetching results...');

        const { items } = await apifyClient.dataset(run.defaultDatasetId).listItems();
        console.log(`📊 Received ${items.length} items from Apify`);

        if (items.length === 0) {
            await interaction.editReply('❌ No data retrieved from Apify. Check if URLs are valid Instagram Reels.');
            return;
        }

        const errorItems = items.filter(item => item.error);
        const validItems = items.filter(item => !item.error && item.videoPlayCount !== undefined);

        console.log(`✅ Valid items: ${validItems.length}, Errors: ${errorItems.length}`);

        if (validItems.length === 0) {
            await interaction.editReply(`❌ No valid Instagram data found. All ${errorItems.length} items had errors.`);
            return;
        }

        const uniqueResults = removeDuplicatesByInputUrl(validItems);

        // Process results with enhanced memory management for Koyeb
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

            // Memory cleanup for large datasets
            if (index % 500 === 0 && global.gc) {
                global.gc();
            }
        });

        console.log(`📊 Processing complete: ${totalVideos} videos, ${totalViews} total views`);

        if (totalVideos === 0) {
            await interaction.editReply('❌ No valid video data found.');
            return;
        }

        // Get views channel for progress calculation
        const viewsChannel = interaction.guild.channels.cache.find(ch => ch.name === CONFIG.VIEWS_CHANNEL_NAME);
        let previousCount = 0;

        if (viewsChannel) {
            const previousCounts = await parseViewsFromChannel(viewsChannel);
            const channelRef = `#${targetChannel.name}`;
            previousCount = previousCounts.get(channelRef) || 0;
            console.log(`📈 Previous count for ${channelRef}: ${previousCount}`);
        }

        // Sort top videos globally
        const allVideos = [];
        pageStats.forEach(stats => {
            allVideos.push(...stats.topVideos);
        });
        allVideos.sort((a, b) => b.views - a.views);
        const top10Videos = allVideos.slice(0, 10);

        // Create comprehensive embed
        const embed = new EmbedBuilder()
            .setTitle('📊 View Count Analysis')
            .setColor(0x0099FF)
            .addFields(
                { name: '**Channel**', value: targetChannel.toString(), inline: true },
                { name: '**Total Videos**', value: totalVideos.toString(), inline: true },
                { name: '**Total Views**', value: totalViews.toLocaleString(), inline: true },
                { name: '**Total Pages**', value: pageStats.size.toString(), inline: true },
                { name: '**Failed URLs**', value: failedUrls.length.toString(), inline: true },
                { name: '**Processing Time**', value: 'Real-time', inline: true }
            )
            .setTimestamp();

        // Add page breakdown (showing more on Koyeb due to better resources)
        let pageBreakdown = '';
        Array.from(pageStats.entries())
            .sort((a, b) => b[1].views - a[1].views)
            .slice(0, 20) // Increased limit for Koyeb
            .forEach(([username, stats]) => {
                pageBreakdown += `@${username} - ${stats.videos} videos - ${formatNumber(stats.views)} views\n`;
            });

        if (pageBreakdown) {
            embed.addFields({ name: '**📋 Top Pages by Views**', value: pageBreakdown, inline: false });
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
        console.log(`📁 Creating Excel file for ${uniqueResults.length} results...`);
        const excelBuffer = await createExcelFile(uniqueResults, targetChannel.name);
        const timestamp = new Date().toISOString().split('T')[0];
        const attachment = new AttachmentBuilder(excelBuffer, { 
            name: `instagram_views_${targetChannel.name}_${timestamp}.xlsx` 
        });

        // Send to views channel
        if (viewsChannel) {
            console.log(`📤 Posting results to #${CONFIG.VIEWS_CHANNEL_NAME}`);
            await viewsChannel.send({ embeds: [embed], files: [attachment] });
        }

        // Calculate progress
        const progressDifference = totalViews - previousCount;
        let progressText = '';
        if (previousCount > 0) {
            const percentageChange = ((progressDifference / previousCount) * 100).toFixed(1);
            progressText = `\n\n**Progress Update:** +${formatNumber(progressDifference)} views (+${percentageChange}%)`;
        }

        // Auto-update progress voice channel
        const progressSettings = await getProgressSettings(interaction.guild);
        if (progressSettings && viewsChannel) {
            console.log(`🎯 Updating progress voice channel...`);
            const channelCounts = await parseViewsFromChannel(viewsChannel);
            const newTotalViews = Array.from(channelCounts.values()).reduce((sum, views) => sum + views, 0);

            await updateProgressVoiceChannel(
                interaction.guild,
                progressSettings.campaignName,
                newTotalViews,
                progressSettings.target
            );
        }

        console.log(`✅ Analysis complete! Sending final response...`);
        await interaction.editReply({ 
            content: `✅ Analysis complete! ${totalVideos} videos analyzed from ${pageStats.size} pages.${progressText}`,
            embeds: [embed], 
            files: [attachment] 
        });

        // Report failed URLs in execution channel
        if (failedUrls.length > 0) {
            const executionChannel = interaction.guild.channels.cache.find(ch => ch.name === CONFIG.EXECUTION_CHANNEL_NAME);
            if (executionChannel) {
                const failedMessage = failedUrls.length > 10 
                    ? `❌ **Failed to process ${failedUrls.length} URLs** (showing first 10):\n${failedUrls.slice(0, 10).join('\n')}`
                    : `❌ **Failed URLs:**\n${failedUrls.join('\n')}`;
                
                await executionChannel.send(failedMessage);
            }
        }

    } catch (error) {
        console.error('❌ Error in viewscount command:', error);
        await interaction.editReply(`❌ An error occurred: ${error.message}`);
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

        console.log(`🎯 Setting up progress tracker for campaign: ${campaignName}`);
        const channelCounts = await parseViewsFromChannel(viewsChannel);
        const totalViews = Array.from(channelCounts.values()).reduce((sum, views) => sum + views, 0);

        const progressChannel = await updateProgressVoiceChannel(
            interaction.guild, 
            campaignName, 
            totalViews, 
            targetViews
        );

        if (progressChannel) {
            const percentage = Math.min((totalViews / targetViews) * 100, 100);
            const remaining = Math.max(targetViews - totalViews, 0);

            const embed = new EmbedBuilder()
                .setTitle('🎯 Progress Tracker Updated')
                .setColor(percentage >= 100 ? 0x00FF00 : 0xFFFF00)
                .addFields(
                    { 
                        name: `**${campaignName} Campaign**`, 
                        value: `**Current:** ${totalViews.toLocaleString()} views\n**Target:** ${targetViews.toLocaleString()} views\n**Progress:** ${percentage.toFixed(1)}%\n**Remaining:** ${remaining.toLocaleString()} views`, 
                        inline: false 
                    },
                    {
                        name: '**Voice Channel Status**',
                        value: `Progress display: ${progressChannel}\nAuto-updates: ✅ Enabled`,
                        inline: false
                    }
                )
                .setTimestamp()
                .setFooter({ text: 'Progress updates automatically with each analysis' });

            // Add progress bar visualization
            const progressBarLength = 20;
            const filledLength = Math.round((percentage / 100) * progressBarLength);
            const emptyLength = progressBarLength - filledLength;
            const progressBar = '█'.repeat(filledLength) + '░'.repeat(emptyLength);
            
            embed.addFields({
                name: '**Progress Bar**',
                value: `\`${progressBar}\` ${percentage.toFixed(1)}%`,
                inline: false
            });

            if (channelCounts.size > 0) {
                let recentUpdates = '';
                let count = 0;
                channelCounts.forEach((views, channel) => {
                    if (count < 15) { // Show more channels on Koyeb
                        recentUpdates += `📈 ${channel}: ${formatNumber(views)} views\n`;
                        count++;
                    }
                });
                embed.addFields({ name: '**Channel Breakdown**', value: recentUpdates, inline: false });
            }

            await interaction.editReply({ embeds: [embed] });
        } else {
            await interaction.editReply('❌ Failed to create/update progress voice channel.');
        }

    } catch (error) {
        console.error('❌ Error in progressbar command:', error);
        await interaction.editReply('❌ An error occurred while updating progress.');
    }
};

// Bot ready event optimized for Koyeb
client.once('ready', async () => {
    console.log(`✅ Bot ready on Koyeb! Logged in as ${client.user.tag}`);
    console.log(`🏠 Serving ${client.guilds.cache.size} guilds with ${client.users.cache.size} users`);
    console.log(`🌐 Health check available at http://localhost:${PORT}`);

    // Register slash commands
    try {
        console.log('🔄 Registering application commands...');
        await client.application.commands.set(commands);
        console.log('✅ Successfully registered all slash commands');
    } catch (error) {
        console.error('❌ Error registering commands:', error);
    }

    // Set up memory cleanup optimized for Koyeb
    setInterval(() => {
        if (global.gc) {
            global.gc();
            console.log('🧹 Memory cleanup performed');
        }
        
        const memUsage = process.memoryUsage();
        console.log(`📊 Memory: ${Math.round(memUsage.heapUsed / 1024 / 1024)}MB heap, ${Math.round(memUsage.rss / 1024 / 1024)}MB RSS`);
        console.log(`⚡ Uptime: ${Math.floor(process.uptime() / 3600)}h ${Math.floor((process.uptime() % 3600) / 60)}m`);
    }, CONFIG.MEMORY_CLEANUP_INTERVAL);

    // Koyeb-specific optimizations
    console.log('🚀 Koyeb optimizations enabled:');
    console.log(`   - Max concurrent requests: ${CONFIG.MAX_CONCURRENT_REQUESTS}`);
    console.log(`   - Request delay: ${CONFIG.REQUEST_DELAY}ms`);
    console.log(`   - Max messages fetch: ${CONFIG.MAX_MESSAGES_FETCH}`);
    console.log(`   - Batch size: ${CONFIG.BATCH_SIZE}`);
});

// Enhanced error handling for Koyeb hosting
process.on('unhandledRejection', (error) => {
    console.error('❌ Unhandled promise rejection:', error);
});

process.on('uncaughtException', (error) => {
    console.error('❌ Uncaught exception:', error);
    // On Koyeb, log but don't exit immediately to allow health checks
    setTimeout(() => process.exit(1), 5000);
});

// Graceful shutdown handlers for Koyeb
process.on('SIGTERM', () => {
    console.log('📴 Received SIGTERM from Koyeb, shutting down gracefully...');
    client.destroy();
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log('📴 Received SIGINT, shutting down gracefully...');
    client.destroy();
    process.exit(0);
});

// Koyeb deployment validation
const validateEnvironment = () => {
    const required = ['DISCORD_TOKEN'];
    const optional = ['APIFY_TOKEN', 'APIFY_TASK_ID'];
    
    console.log('🔍 Environment validation:');
    
    for (const env of required) {
        if (!process.env[env]) {
            console.error(`❌ Required environment variable ${env} is missing`);
            process.exit(1);
        } else {
            console.log(`✅ ${env}: Set`);
        }
    }
    
    for (const env of optional) {
        if (process.env[env]) {
            console.log(`✅ ${env}: Set`);
        } else {
            console.log(`⚠️ ${env}: Not set (optional)`);
        }
    }
};

// Run environment validation
validateEnvironment();

// Enhanced login with retry logic for Koyeb
const loginWithRetry = async (maxRetries = 3) => {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            console.log(`🔐 Login attempt ${attempt}/${maxRetries}...`);
            await client.login(process.env.DISCORD_TOKEN);
            console.log('✅ Successfully logged in to Discord');
            return;
        } catch (error) {
            console.error(`❌ Login attempt ${attempt} failed:`, error.message);
            
            if (attempt === maxRetries) {
                console.error('❌ All login attempts failed. Exiting...');
                process.exit(1);
            }
            
            // Wait before retry (exponential backoff)
            const delay = Math.pow(2, attempt) * 1000;
            console.log(`⏳ Waiting ${delay}ms before retry...`);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
};

// Start the bot
loginWithRetry().catch(error => {
    console.error('❌ Failed to start bot:', error);
    process.exit(1);
});
