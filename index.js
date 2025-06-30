const { Client, GatewayIntentBits, SlashCommandBuilder, EmbedBuilder, AttachmentBuilder, PermissionFlagsBits } = require('discord.js');
const { ApifyClient } = require('apify-client');
const ExcelJS = require('exceljs');
const express = require('express');

// Initialize Express app for Koyeb health checks
const app = express();
const PORT = process.env.PORT || 8000;

// Health check endpoints for Koyeb
app.get('/', (req, res) => {
    res.status(200).json({
        status: 'Bot is running',
        uptime: process.uptime(),
        memory: Math.round(process.memoryUsage().heapUsed / 1024 / 1024) + 'MB',
        timestamp: new Date().toISOString(),
        bot_ready: client.isReady()
    });
});

app.get('/health', (req, res) => {
    const isReady = client.isReady();
    res.status(isReady ? 200 : 503).json({
        status: isReady ? 'healthy' : 'unhealthy',
        bot_ready: isReady,
        ws_ping: client.ws.ping,
        uptime: process.uptime(),
        guilds: client.guilds.cache.size
    });
});

// Initialize Discord client
const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.MessageContent,
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
    PROGRESS_VOICE_CHANNEL_PREFIX: '📊 Progress: '
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
                // Clean URL but preserve case for short code
                let cleanUrl = url
                    .replace(/\/+$/, '') // Remove trailing slashes
                    .replace(/\?.*$/, '') // Remove query parameters  
                    .replace(/#.*$/, ''); // Remove hash fragments

                // Ensure www.instagram.com format (Instagram API requirement)
                cleanUrl = cleanUrl.replace(/https?:\/\/(?:www\.)?instagram\.com/, 'https://www.instagram.com');

                // Add trailing slash for consistency
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
        // Create normalized identifier for comparison (case-insensitive)
        const normalizedInputUrl = item.inputUrl
            ?.toLowerCase()
            .replace(/\/+$/, '')
            .replace(/\?.*$/, '')
            .replace(/#.*$/, '')
            .replace(/www\./, '');

        const shortCode = item.shortCode?.toLowerCase(); // Case-insensitive comparison
        const videoId = item.id;

        // Use multiple identifiers to catch duplicates
        const identifiers = [
            normalizedInputUrl,
            shortCode,
            videoId
        ].filter(Boolean);

        // Check if any identifier has been seen before
        const isDuplicate = identifiers.some(identifier => seen.has(identifier));

        if (!isDuplicate) {
            // Mark all identifiers as seen
            identifiers.forEach(identifier => seen.add(identifier));
            uniqueResults.push(item);
        } else {
            console.log(`Duplicate detected: ${item.inputUrl} (Short Code: ${item.shortCode})`);
        }
    });

    console.log(`Removed ${results.length - uniqueResults.length} duplicates from Apify results`);
    return uniqueResults;
};

const parseViewsFromChannel = async (channel) => {
    const channelCounts = new Map();

    try {
        // Fetch ALL messages from the channel
        let allMessages = [];
        let lastMessageId = null;

        while (true) {
            const options = { limit: 100 };
            if (lastMessageId) {
                options.before = lastMessageId;
            }

            const messages = await channel.messages.fetch(options);
            if (messages.size === 0) break;

            allMessages.push(...Array.from(messages.values()));
            lastMessageId = messages.last().id;

            // Small delay to avoid rate limits
            if (messages.size === 100) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }

        console.log(`Fetched ${allMessages.length} total messages from #${channel.name}`);

        allMessages.forEach(message => {
            if (message.embeds.length > 0) {
                const embed = message.embeds[0];

                // Check if this is a view count analysis embed
                if (embed.title && embed.title.includes('View Count Analysis')) {
                    let channelName = '';
                    let totalViews = 0;

                    // Method 1: Try to extract from embed fields
                    if (embed.fields && embed.fields.length > 0) {
                        embed.fields.forEach(field => {
                            if (field.name === '**Channel**') {
                                channelName = field.value.replace(/[<>#]/g, ''); // Remove Discord formatting
                            }
                            if (field.name === '**Total Views**') {
                                totalViews = parseInt(field.value.replace(/,/g, ''));
                            }
                        });
                    }

                    // Method 2: Try to extract from description if fields method failed
                    if (!channelName && embed.description) {
                        const channelMatch = embed.description.match(/\*\*Channel\*\*\s*[<#]*(\w+)[>#]*/);
                        const viewsMatch = embed.description.match(/\*\*Total Views\*\*\s*([\d,]+)/);

                        if (channelMatch) channelName = channelMatch[1];
                        if (viewsMatch) totalViews = parseInt(viewsMatch[1].replace(/,/g, ''));
                    }

                    // Store the data if both channel and views found
                    if (channelName && totalViews > 0) {
                        // Ensure channel name starts with #
                        if (!channelName.startsWith('#')) {
                            channelName = '#' + channelName;
                        }

                        // Always update with latest count (messages are fetched newest first)
                        // So the first occurrence we find is the most recent
                        if (!channelCounts.has(channelName)) {
                            channelCounts.set(channelName, totalViews);
                            console.log(`Found latest count for ${channelName}: ${totalViews} views`);
                        }
                        // Skip older messages for the same channel since we already have the latest
                    }
                }
            }
        });
    } catch (error) {
        console.error('Error parsing views channel:', error);
    }

    console.log('Parsed channel counts:', channelCounts);
    return channelCounts;
};

const updateProgressVoiceChannel = async (guild, campaignName, currentViews, targetViews) => {
    try {
        const percentage = Math.min((currentViews / targetViews) * 100, 100);
        const channelName = `${CONFIG.PROGRESS_VOICE_CHANNEL_PREFIX}${campaignName} (${formatNumber(currentViews)}/${formatNumber(targetViews)}) ${percentage.toFixed(1)}%`;

        // Find existing progress voice channel
        let progressChannel = guild.channels.cache.find(ch => 
            ch.type === 2 && // Voice channel type
            ch.name.startsWith(CONFIG.PROGRESS_VOICE_CHANNEL_PREFIX)
        );

        if (progressChannel) {
            // Update existing channel name
            await progressChannel.setName(channelName);
            console.log(`Updated progress voice channel: ${channelName}`);
        } else {
            // Create new voice channel
            progressChannel = await guild.channels.create({
                name: channelName,
                type: 2, // Voice channel
                permissionOverwrites: [
                    {
                        id: guild.roles.everyone,
                        deny: ['Connect', 'Speak'], // Everyone can see but not join/speak
                    },
                ],
            });
            console.log(`Created new progress voice channel: ${channelName}`);
        }

        return progressChannel;
    } catch (error) {
        console.error('Error updating progress voice channel:', error);
        return null;
    }
};

const getProgressSettings = async (guild) => {
    try {
        // Look for existing progress voice channel to extract settings
        const progressChannel = guild.channels.cache.find(ch => 
            ch.type === 2 && 
            ch.name.startsWith(CONFIG.PROGRESS_VOICE_CHANNEL_PREFIX)
        );

        if (progressChannel) {
            const channelName = progressChannel.name;
            // Extract campaign name and target from channel name
            const match = channelName.match(/📊 Progress: (.+?) \([\d.,KM]+\/([\d.,KM]+)\)/);
            if (match) {
                const campaignName = match[1];
                const targetStr = match[2];

                // Convert target back to number
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
        console.error('Error getting progress settings:', error);
        return null;
    }
};

const createExcelFile = async (results, channelName) => {
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Instagram Views Data');

    // Add headers
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

    // Add data
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

    // Style the header row
    worksheet.getRow(1).font = { bold: true };
    worksheet.getRow(1).fill = {
        type: 'pattern',
        pattern: 'solid',
        fgColor: { argb: 'FFE0E0E0' }
    };

    // Generate buffer
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

// Command handlers
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
        console.error(`Error handling command ${commandName}:`, error);
        if (!interaction.replied && !interaction.deferred) {
            await interaction.reply('❌ An error occurred while processing your command.');
        } else {
            await interaction.editReply('❌ An error occurred while processing your command.');
        }
    }
});

const handleStatus = async (interaction) => {
    await interaction.deferReply();

    const embed = new EmbedBuilder()
        .setTitle('🤖 Bot Status')
        .setColor(0x00FF00)
        .addFields(
            { name: '🟢 Status', value: 'Online', inline: true },
            { name: '📊 Memory Usage', value: `${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)} MB`, inline: true },
            { name: '⏱️ Uptime', value: `${Math.floor(process.uptime() / 3600)}h ${Math.floor((process.uptime() % 3600) / 60)}m`, inline: true },
            { name: '🔧 Environment', value: 'Koyeb Hosting', inline: true },
            { name: '📡 Ping', value: `${client.ws.ping}ms`, inline: true },
            { name: '🏠 Guilds', value: client.guilds.cache.size.toString(), inline: true }
        )
        .setTimestamp();

    await interaction.editReply({ embeds: [embed] });
};

const handleViewsCount = async (interaction) => {
    const targetChannel = interaction.options.getChannel('channel');

    await interaction.deferReply();

    try {
        // Extract URLs from target channel - fetch ALL messages
        let allMessages = [];
        let lastMessageId = null;

        while (true) {
            const options = { limit: 100 };
            if (lastMessageId) {
                options.before = lastMessageId;
            }

            const messages = await targetChannel.messages.fetch(options);
            if (messages.size === 0) break;

            allMessages.push(...Array.from(messages.values()));
            lastMessageId = messages.last().id;

            // Optional: Add small delay to avoid rate limits
            if (messages.size === 100) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }

        const urls = extractInstagramUrls(allMessages);

        if (urls.length === 0) {
            await interaction.editReply('❌ No Instagram URLs found in the specified channel.');
            return;
        }

        await interaction.editReply(`🔍 Found ${urls.length} unique URLs. Processing with Apify...`);

        // Prepare Apify input with exact format from your task
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

        if (errorItems.length > 0) {
            console.log(`- Error details:`, errorItems[0]);
        }

        if (validItems.length === 0) {
            await interaction.editReply(`❌ No valid Instagram data found. Possible reasons:
• Videos are private or deleted
• URLs are incorrect format  
• Instagram is blocking requests
• Try with different/newer Reels URLs`);
            return;
        }

        // Log first valid item structure for debugging
        console.log(`- First valid item structure:`, JSON.stringify(validItems[0], null, 2));

        // Remove duplicates by inputUrl (only process valid items)
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
            console.log(`Processing item ${index + 1}:`, {
                url: item.inputUrl,
                username: item.ownerUsername,
                views: item.videoPlayCount,
                shortCode: item.shortCode
            });

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
                console.log(`✅ Processed: @${username} - ${views} views`);
            } else {
                console.log(`❌ Skipped item due to missing data:`, {
                    hasViews: item.videoPlayCount !== undefined,
                    hasUsername: !!item.ownerUsername,
                    url: item.inputUrl
                });
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
            // Create proper channel reference for lookup
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
        console.log(`✅ Excel file created successfully`);

        // Send to views channel if it exists
        if (viewsChannel) {
            console.log(`📤 Sending results to #${CONFIG.VIEWS_CHANNEL_NAME}...`);
            await viewsChannel.send({ embeds: [embed], files: [attachment] });
            console.log(`✅ Results posted to views channel`);
        } else {
            console.log(`⚠️ Views channel #${CONFIG.VIEWS_CHANNEL_NAME} not found`);
        }

        // Calculate progress update
        const progressDifference = totalViews - previousCount;
        let progressText = '';
        if (previousCount > 0) {
            progressText = `\n\n**Progress Update:** +${formatNumber(progressDifference)} views from previous count`;
        }

        console.log(`📊 Progress calculation: ${totalViews} - ${previousCount} = +${progressDifference}`);

        // Auto-update progress voice channel if it exists
        const progressSettings = await getProgressSettings(interaction.guild);
        if (progressSettings) {
            console.log(`🎯 Updating progress voice channel...`);
            const channelCounts = await parseViewsFromChannel(viewsChannel);
            const newTotalViews = Array.from(channelCounts.values()).reduce((sum, views) => sum + views, 0);

            await updateProgressVoiceChannel(
                interaction.guild,
                progressSettings.campaignName,
                newTotalViews,
                progressSettings.target
            );
            console.log(`✅ Progress voice channel updated`);
        }

        console.log(`🎉 Sending final response to user...`);
        await interaction.editReply({ 
            content: `✅ Analysis complete! Results posted to #${CONFIG.VIEWS_CHANNEL_NAME}${progressText}`,
            embeds: [embed], 
            files: [attachment] 
        });
        console.log(`✅ Command completed successfully!`);

        // Report failed URLs if any
        if (failedUrls.length > 0) {
            const executionChannel = interaction.guild.channels.cache.find(ch => ch.name === CONFIG.EXECUTION_CHANNEL_NAME);
            if (executionChannel) {
                await executionChannel.send(`❌ **Failed URLs:**\n${failedUrls.join('\n')}`);
            }
        }

    } catch (error) {
        console.error('Error in viewscount command:', error);
        await interaction.editReply('❌ An error occurred while processing the request.');
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

    // Register slash commands
    try {
        console.log('🔄 Started refreshing application (/) commands.');
        await client.application.commands.set(commands);
        console.log('✅ Successfully reloaded application (/) commands.');
    } catch (error) {
        console.error('❌ Error registering commands:', error);
    }
});

// Error handling
process.on('unhandledRejection', error => {
    console.error('Unhandled promise rejection:', error);
});

process.on('uncaughtException', error => {
    console.error('Uncaught exception:', error);
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('Received SIGTERM, shutting down gracefully...');
    client.destroy();
    process.exit(0);
});

// Start Express server first
app.listen(PORT, '0.0.0.0', () => {
    console.log(`🌐 Health check server running on port ${PORT}`);
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
